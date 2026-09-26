-- jev 0.3.0 — natural-language predicates for PostgreSQL, powered by TypeSafe's Jev.
--
--   SELECT * FROM people WHERE jev(people, 'name is european');
--   SELECT name, jev_prob(people, 'works in healthcare') AS p FROM people;
--   SELECT name, jev_score(products, 'how luxurious is this product', ARRAY['budget','mid-range','luxury']) FROM products;
--   SELECT subject, jev_choice(support_tickets, 'which team should handle this', ARRAY['billing','technical','sales']) FROM support_tickets;
--
-- No index is needed. Rows are judged by the model, not matched by pattern.
--
-- How a statement runs: the first call for a table + question starts a read-ahead that streams the
-- table in physical order (pages of rows, constant memory, any table size), packs jev.batch_size rows
-- into one API request (one shared state, one question per row) and keeps jev.concurrency requests in
-- flight over persistent HTTPS connections. Rows are answered as their batch returns, so the executor
-- never waits for the whole table, a LIMIT stops the read-ahead early, and rows that other predicates
-- filter out before jev() is called are skipped rather than judged. Answers are cached per row content
-- for the session, so re-running a query, changing the threshold or sorting by probability is free.
--
-- Settings (SET jev.<name> = ...):
--   jev.api_key            TypeSafe API key (falls back to the TYPESAFE_API_KEY env var of the server)
--   jev.model              default 'jev-latest'
--   jev.threshold          default 0.5   probability at which jev() returns true
--   jev.batch_size         default 20    rows per API request (accuracy drops measurably above ~20-25 rows)
--   jev.concurrency        default 16    parallel API requests
--   jev.max_prefetch_rows  default 5000  how far past a cache miss the read-ahead scans to find the row, and
--                                        how many skipped rows it keeps for later (memory bound)
--   jev.notices            default 'on'  emit progress NOTICEs and a summary per table read-ahead
--   jev.api_url            default 'https://api.typesafe.ai/v1/systemone' (proxies, mocks, tests)
--   jev.timeout            default 30    seconds per API request
--   jev.keepalive          default 600   seconds a pooled API connection may sit idle before it is reconnected
--   jev.max_rows_per_statement   default 0 (off)  never send more rows than this to the API in one statement
--   jev.max_chars_per_statement  default 0 (off)  never send more characters of row data than this in one statement
--                                                 Both are spend guards for shared or public deployments.

\echo Use "CREATE EXTENSION jev" to load this file. \quit

CREATE OR REPLACE FUNCTION _jev_eval(rel_type text, row_json text, query text, kind text, options text)
RETURNS jsonb
LANGUAGE plpython3u
STABLE
AS $py$
import json, os, time, hashlib, threading, random, ssl, socket, select, http.client
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from urllib.parse import urlsplit

USD_PER_INPUT_TOKEN = 0.042 / 1_000_000  # jev-1.13 list price; output tokens are free
PAGE_ROWS = 1000                          # rows read from the table per SPI query
STATE_VERSION = 2

# ---------------------------------------------------------------- session state (survives across calls)
if GD.get("jev", {}).get("version") != STATE_VERSION:
    GD["jev"] = {
        "version": STATE_VERSION,
        "cache": {},          # cache_key -> {row_hash: answer}
        "jobs": {},           # cache_key -> read-ahead state for one relation + question
        "stats": {"requests": 0, "input_tokens": 0, "output_tokens": 0, "rows_evaluated": 0,
                  "cache_hits": 0, "api_ms": 0.0, "batches": 0, "errors": 0, "retries": 0},
        "plans": {},
        "lock": threading.Lock(),
        "pool": None, "pool_size": 0,   # ThreadPoolExecutor shared by all jobs of this session
        "conns": {},                    # (scheme, host, port) -> [idle keep-alive connections]
        "stmt": {"ts": None, "rows": 0, "chars": 0},   # per-statement spend guard
    }
S = GD["jev"]
LOCK = S["lock"]

def plan(name, sql, types):
    p = S["plans"].get(name)
    if p is None:
        p = S["plans"][name] = plpy.prepare(sql, types)
    return p

# ---------------------------------------------------------------- settings: one SPI round trip per cache miss
CFG_SQL = """SELECT statement_timestamp()::text AS ts,
  current_setting('jev.api_key', true) AS api_key, current_setting('jev.model', true) AS model,
  current_setting('jev.batch_size', true) AS batch_size, current_setting('jev.concurrency', true) AS concurrency,
  current_setting('jev.max_prefetch_rows', true) AS max_prefetch_rows, current_setting('jev.notices', true) AS notices,
  current_setting('jev.api_url', true) AS api_url, current_setting('jev.timeout', true) AS timeout,
  current_setting('jev.keepalive', true) AS keepalive,
  current_setting('jev.max_rows_per_statement', true) AS max_rows, current_setting('jev.max_chars_per_statement', true) AS max_chars"""

def load_cfg():
    r = plpy.execute(plan("cfg", CFG_SQL, []))[0]
    def g(name, default):
        v = r[name]
        return default if v in (None, "") else v
    key = g("api_key", None) or os.environ.get("TYPESAFE_API_KEY")
    if not key:
        plpy.error("jev: no API key. SET jev.api_key = '...' or start the server with TYPESAFE_API_KEY set.")
    url = urlsplit(g("api_url", "https://api.typesafe.ai/v1/systemone"))
    return {
        "ts": r["ts"], "api_key": key, "model": g("model", "jev-latest"),
        "batch_size": max(1, int(g("batch_size", "20"))), "concurrency": max(1, int(g("concurrency", "16"))),
        "max_prefetch": max(1, int(g("max_prefetch_rows", "5000"))),
        "notices": g("notices", "on").lower() in ("on", "true", "1", "yes"),
        "timeout": float(g("timeout", "30")), "keepalive": float(g("keepalive", "600")),
        "max_rows": int(g("max_rows", "0")), "max_chars": int(g("max_chars", "0")),
        "scheme": url.scheme, "host": url.hostname, "port": url.port,
        "path": (url.path or "/") + ("?" + url.query if url.query else ""),
    }

# ---------------------------------------------------------------- question builders
opts = json.loads(options) if options else None

def build_question(i):
    ref = "rows[%d]" % i
    if kind == "noul":
        return {"type": "noul",
                "instructions": "Does the record `%s` satisfy the condition stated in `condition`?" % ref}
    if kind == "score":
        return {"type": "score", "instructions": "Rate the record `%s`: %s" % (ref, query), "criteria": opts}
    if kind == "choice":
        return {"type": "choice", "instructions": "For the record `%s`: %s" % (ref, query),
                "criteria": {o: None for o in opts}}
    raise RuntimeError("jev: unknown kind %r" % kind)

def request_body(cfg, rows):
    state = {"condition": query, "rows": rows} if kind == "noul" else {"rows": rows}
    return json.dumps({"model": cfg["model"], "state": state,
                       "questions": {("r%d" % i): build_question(i) for i in range(len(rows))}}).encode()

# ---------------------------------------------------------------- HTTP: persistent connections, retries (threads: no plpy here)
def conn_key(cfg):
    return (cfg["scheme"], cfg["host"], cfg["port"])

def alive(c):
    """An idle keep-alive connection never has unread data: readability means EOF or a TLS close alert."""
    sock = c.sock
    if sock is None:
        return False
    try:
        readable, _, _ = select.select([sock], [], [], 0)
        return not readable
    except (OSError, ValueError):
        return False

def borrow_conn(cfg):
    while True:
        with LOCK:
            idle = S["conns"].setdefault(conn_key(cfg), [])
            entry = idle.pop() if idle else None
        if entry is None:
            break
        c, last_used = entry
        if time.time() - last_used < cfg["keepalive"] and alive(c):
            return c, True
        c.close()                            # idle too long or closed by the server: never send into a dead socket
    if cfg["scheme"] == "https":
        if S.get("ssl_ctx") is None:
            S["ssl_ctx"] = ssl.create_default_context()   # loading the CA bundle once per session, not per connection
        c = http.client.HTTPSConnection(cfg["host"], cfg["port"], timeout=cfg["timeout"], context=S["ssl_ctx"])
    else:
        c = http.client.HTTPConnection(cfg["host"], cfg["port"], timeout=cfg["timeout"])
    return c, False

def release_conn(cfg, c, reusable):
    if not reusable:
        c.close(); return
    with LOCK:
        idle = S["conns"].setdefault(conn_key(cfg), [])
        if len(idle) < cfg["concurrency"]:
            idle.append((c, time.time())); return
    c.close()

def tcp_keepalive(c):
    """Let the kernel notice a silently dropped peer within about a minute (probes after 30 s idle, every 10 s,
    3 misses), so a pooled connection that a NAT or the server dropped without a FIN fails fast instead of
    stalling a request until jev.timeout."""
    try:
        sock = c.sock
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for name, value in (("TCP_KEEPIDLE", 30), ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 3)):
            if hasattr(socket, name):
                sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, name), value)
    except OSError:
        pass

def retry_after_seconds(resp):
    v = resp.getheader("retry-after-ms")
    if v and v.strip().isdigit():
        return int(v) / 1000.0
    v = resp.getheader("retry-after")
    if v:
        try:
            return float(v)
        except ValueError:
            pass
    return None

def call_api(cfg, rows):
    body = request_body(cfg, rows)
    headers = {"Authorization": "Bearer " + cfg["api_key"], "Content-Type": "application/json",
               "User-Agent": "pg-jev/0.3.0"}
    delay, last = 0.5, None
    for attempt in range(7):
        c, reused = borrow_conn(cfg)
        t0 = time.time()
        try:
            if not reused:
                c.connect()
                tcp_keepalive(c)
            c.request("POST", cfg["path"], body=body, headers=headers)
            resp = c.getresponse()
            raw = resp.read()
        except (http.client.HTTPException, OSError) as e:
            c.close()
            last = "%s: %s" % (type(e).__name__, e)
            with LOCK:
                S["stats"]["retries"] += 1
            if reused and attempt == 0:
                continue                       # a keep-alive connection went stale: retry at once on a fresh one
            time.sleep(delay + random.random() * 0.25); delay = min(delay * 2, 8)
            continue
        ms = (time.time() - t0) * 1000
        release_conn(cfg, c, not resp.will_close)
        if resp.status == 200:
            data = json.loads(raw.decode())
            data["_ms"] = ms
            return data
        last = "%s %s" % (resp.status, raw.decode(errors="replace")[:300])
        if resp.status in (408, 429, 529) or resp.status >= 500:
            with LOCK:
                S["stats"]["retries"] += 1
            wait = retry_after_seconds(resp)
            time.sleep(min(wait if wait is not None else delay, 30) + random.random() * 0.25)
            delay = min(delay * 2, 8)
            continue
        raise RuntimeError("jev: TypeSafe API error " + last)
    raise RuntimeError("jev: TypeSafe API unreachable after retries: " + str(last))

def run_batch(cfg, job, bucket, pairs):
    """Worker thread: judge one batch and store the answers. Returns the number of rows judged."""
    try:
        data = call_api(cfg, [json.loads(t) for _, t in pairs])
    except Exception:
        with LOCK:
            S["stats"]["errors"] += 1
        raise
    answers = data.get("answers") or {}
    if any(("r%d" % i) not in answers for i in range(len(pairs))):
        with LOCK:
            S["stats"]["errors"] += 1
        raise RuntimeError("jev: TypeSafe API response is missing answers (%d of %d)" % (len(answers), len(pairs)))
    usage = data.get("usage", {})
    with LOCK:
        for i, (h, _) in enumerate(pairs):
            bucket[h] = answers["r%d" % i]
        st = S["stats"]
        st["requests"] += 1
        st["input_tokens"] += usage.get("input_tokens", 0)
        st["output_tokens"] += usage.get("output_tokens", 0)
        st["rows_evaluated"] += len(pairs)
        st["api_ms"] += data["_ms"]
        if job is not None:
            job["done_reqs"] += 1
            job["done_rows"] += len(pairs)
            job["tokens"] += usage.get("input_tokens", 0)
    return len(pairs)

def pool(cfg):
    if S["pool"] is None or S["pool_size"] != cfg["concurrency"]:
        if S["pool"] is not None:
            S["pool"].shutdown(wait=False)
        S["pool"] = ThreadPoolExecutor(max_workers=cfg["concurrency"], thread_name_prefix="jev")
        S["pool_size"] = cfg["concurrency"]
    return S["pool"]

# ---------------------------------------------------------------- spend guard
def guard(cfg, pairs):
    """Refuse to send a batch that would push this statement over the configured limits."""
    st = S["stmt"]
    if st["ts"] != cfg["ts"]:
        st["ts"], st["rows"], st["chars"] = cfg["ts"], 0, 0
    rows, chars = st["rows"] + len(pairs), st["chars"] + sum(len(t) for _, t in pairs)
    if cfg["max_rows"] and rows > cfg["max_rows"]:
        plpy.error("jev: this statement would send %d rows to the API, above jev.max_rows_per_statement = %d"
                   % (rows, cfg["max_rows"]))
    if cfg["max_chars"] and chars > cfg["max_chars"]:
        plpy.error("jev: this statement would send %d characters of row data to the API, above jev.max_chars_per_statement = %d"
                   % (chars, cfg["max_chars"]))
    st["rows"], st["chars"] = rows, chars

# ---------------------------------------------------------------- waiting that stays cancellable
def wait_for(fut):
    """Block on a request; poke SPI every 250 ms so statement_timeout and cancel requests get through."""
    while True:
        try:
            return fut.result(timeout=0.25)
        except FutureTimeout:
            plpy.execute(plan("noop", "SELECT 1", []))

# Semantic views demand exactly one row. Use a private, throwaway answer bucket:
# transaction rollback and model changes must never reuse the legacy session cache.
if rel_type == "__jev_demand__":
    if kind != "noul" or not isinstance(opts, dict) or not opts.get("model"):
        plpy.error("jev: invalid demanded-row evaluation")
    cfg = load_cfg()
    cfg["model"] = opts["model"]
    url = urlsplit(opts["api_url"])
    cfg.update(scheme=url.scheme, host=url.hostname, port=url.port,
               path=(url.path or "/") + ("?" + url.query if url.query else ""))
    pairs = [("row", row_json)]
    guard(cfg, pairs)
    demanded = {}
    future = pool(cfg).submit(run_batch, cfg, None, demanded, pairs)
    try:
        wait_for(future)
    except RuntimeError as e:
        plpy.error(str(e))
    return json.dumps(demanded["row"])

# ---------------------------------------------------------------- read-ahead job: streams one relation in physical order
REL_SQL = """SELECT c.oid::regclass::text AS rel, c.relkind, c.reltuples,
  pg_relation_size(c.oid) / current_setting('block_size')::int AS nblocks
  FROM pg_class c WHERE c.oid = to_regclass($1)"""

def new_job(cfg):
    job = {"cfg": cfg, "rel": None, "mode": None, "futures": [], "inflight": {}, "fetched": deque(),
           "skipped": deque(), "skipped_map": {}, "exhausted": True, "restart_ts": cfg["ts"],
           "done_reqs": 0, "done_rows": 0, "tokens": 0, "reported_reqs": 0, "summary_done": False, "t0": time.time(),
           "est_rows": 0, "block": 0, "nblocks": 0, "rows_per_block": 50.0, "offset": 0}
    info = plpy.execute(plan("rel", REL_SQL, ["text"]), [rel_type])
    if info.nrows():
        r = info[0]
        if r["relkind"] in ("r", "m"):
            job.update(rel=r["rel"], mode="ctid", nblocks=int(r["nblocks"]), exhausted=False)
            if r["reltuples"] and r["reltuples"] > 0:
                job["est_rows"] = int(r["reltuples"])
                job["rows_per_block"] = max(1.0, r["reltuples"] / max(1, job["nblocks"]))
        elif r["relkind"] in ("v", "p", "f"):
            job.update(rel=r["rel"], mode="offset", exhausted=False)
        if job["rel"] and not job["est_rows"]:
            job["est_rows"] = plpy.execute("SELECT count(*) AS n FROM %s" % job["rel"])[0]["n"]
    return job

def restart(job, cfg):
    job.update(cfg=cfg, exhausted=False, restart_ts=cfg["ts"], block=0, offset=0, t0=time.time(),
               done_reqs=0, done_rows=0, tokens=0, reported_reqs=0, summary_done=False)
    job["fetched"].clear()

def fetch_page(job):
    """Append the next page of (row_hash, row_json) pairs to job['fetched']; sets 'exhausted' at the end."""
    if job["exhausted"]:
        return 0
    if job["mode"] == "ctid":
        if job["block"] >= job["nblocks"]:
            job["exhausted"] = True
            return 0
        blocks = max(1, min(job["nblocks"] - job["block"], int(PAGE_ROWS / job["rows_per_block"]) + 1))
        lo, hi = job["block"], job["block"] + blocks
        rows = plpy.execute(plan("page:" + job["rel"],
                                 "SELECT to_json(t)::text AS r FROM %s t WHERE ctid >= $1 AND ctid < $2" % job["rel"],
                                 ["tid", "tid"]), ["(%d,0)" % lo, "(%d,0)" % hi])
        job["block"] = hi
        if rows.nrows():
            job["rows_per_block"] = max(1.0, 0.7 * job["rows_per_block"] + 0.3 * rows.nrows() / blocks)
    else:
        rows = plpy.execute(plan("page:" + job["rel"],
                                 "SELECT to_json(t)::text AS r FROM %s t OFFSET $1 LIMIT $2" % job["rel"],
                                 ["bigint", "bigint"]), [job["offset"], PAGE_ROWS])
        job["offset"] += rows.nrows()
        if rows.nrows() < PAGE_ROWS:
            job["exhausted"] = True
    for r in rows:
        t = r["r"]
        job["fetched"].append((hashlib.sha1(t.encode()).hexdigest(), t))
    return rows.nrows()

def skip(job, pair):
    """Remember a row the executor passed over, so a later request for it can still be batched."""
    h, t = pair
    if h in job["skipped_map"]:
        return
    job["skipped_map"][h] = t
    job["skipped"].append(h)
    while len(job["skipped"]) > job["cfg"]["max_prefetch"]:
        job["skipped_map"].pop(job["skipped"].popleft(), None)

def submit(job, cfg, bucket, pairs):
    pairs = [p for p in pairs if p[0] not in bucket and p[0] not in job["inflight"]]
    if not pairs:
        return None
    guard(cfg, pairs)
    fut = pool(cfg).submit(run_batch, cfg, job, bucket, pairs)
    fut.jev_hashes = [h for h, _ in pairs]
    job["futures"].append(fut)
    for h, _ in pairs:
        job["inflight"][h] = fut
        job["skipped_map"].pop(h, None)
    return fut

def top_up(job, cfg, bucket):
    """Keep up to 2 x concurrency requests in flight, in physical order, without exceeding the page buffer."""
    cap = 2 * cfg["concurrency"]
    while len(job["futures"]) < cap:
        while len(job["fetched"]) < cfg["batch_size"] and fetch_page(job):
            pass                                    # full batches across page boundaries
        if not job["fetched"]:
            return
        batch = [job["fetched"].popleft() for _ in range(min(cfg["batch_size"], len(job["fetched"])))]
        submit(job, cfg, bucket, batch)

def locate(job, cfg, h):
    """Scan forward from the read-ahead position until row h is at the head of job['fetched'].
    Rows passed over are kept in 'skipped'. Returns False when h is not within cfg['max_prefetch'] rows."""
    scanned = 0
    while True:
        while job["fetched"]:
            if job["fetched"][0][0] == h:
                return True
            skip(job, job["fetched"].popleft())
            scanned += 1
        if scanned >= cfg["max_prefetch"] or not fetch_page(job):
            return False

def sweep(job, cfg):
    """Main thread bookkeeping for finished requests: progress notices and the end-of-table summary."""
    still = []
    for fut in job["futures"]:
        if fut.done():
            for h in fut.jev_hashes:           # answered rows are in the cache; failed rows get retried on request
                job["inflight"].pop(h, None)
        else:
            still.append(fut)
    job["futures"] = still
    if not cfg["notices"] or job["rel"] is None:
        return
    if job["done_reqs"] > job["reported_reqs"]:
        job["reported_reqs"] = job["done_reqs"]
        total_reqs = max(job["done_reqs"], -(-job["est_rows"] // cfg["batch_size"]))
        plpy.notice("jev: progress %d/%d requests, %d/%d rows"
                    % (job["done_reqs"], total_reqs, job["done_rows"], max(job["done_rows"], job["est_rows"])))
    if job["exhausted"] and not job["fetched"] and not job["futures"] and not job["summary_done"] and job["done_rows"]:
        job["summary_done"] = True
        n, r, tok = job["done_rows"], job["done_reqs"], job["tokens"]
        plpy.notice("jev: %s → judged %d row%s of %s in %d request%s, %d input tokens (≈$%.4f), %.0f ms"
                    % (kind, n, "" if n == 1 else "s", job["rel"], r, "" if r == 1 else "s",
                       tok, tok * USD_PER_INPUT_TOKEN, (time.time() - job["t0"]) * 1000))

def answer(bucket, h, fut):
    try:
        wait_for(fut)
    except RuntimeError as e:
        plpy.error(str(e))
    if h not in bucket:
        plpy.error("jev: the API returned no answer for a row")
    return json.dumps(bucket[h])

# ---------------------------------------------------------------- main
if kind not in ("noul", "score", "choice"):
    plpy.error("jev: unknown kind %r" % kind)
if kind != "noul" and not isinstance(opts, list):
    plpy.error("jev: %s needs a text[] of %s" % (kind, "levels" if kind == "score" else "options"))
cache_key = json.dumps([rel_type, query, kind, opts], sort_keys=True)
bucket = S["cache"].setdefault(cache_key, {})
row_hash = hashlib.sha1(row_json.encode()).hexdigest()
job = S["jobs"].get(cache_key)

if row_hash in bucket:
    S["stats"]["cache_hits"] += 1
    if job is not None and job["futures"]:
        sweep(job, job["cfg"])
        if len(job["futures"]) < job["cfg"]["concurrency"] and (job["fetched"] or not job["exhausted"]):
            cfg = load_cfg()
            if cfg["ts"] == job["cfg"]["ts"]:       # same statement: keep the pipeline full while the executor consumes hits
                top_up(job, cfg, bucket)
    return json.dumps(bucket[row_hash])

cfg = load_cfg()
if job is None:
    job = S["jobs"][cache_key] = new_job(cfg)
else:
    job["cfg"] = cfg
    sweep(job, cfg)

fut = job["inflight"].get(row_hash)
if fut is None and job["rel"] is not None:
    if row_hash in job["skipped_map"]:
        # The executor came back for a row the read-ahead passed over (index scan, backward scan, join order):
        # judge it together with the most recently skipped rows, which are its likely neighbours.
        pairs = [(row_hash, job["skipped_map"].pop(row_hash))]
        while job["skipped"] and len(pairs) < cfg["batch_size"]:
            h = job["skipped"].pop()
            if h in job["skipped_map"]:
                pairs.append((h, job["skipped_map"].pop(h)))
        fut = submit(job, cfg, bucket, pairs)
    else:
        found = locate(job, cfg, row_hash)
        if not found and job["exhausted"] and job["restart_ts"] != cfg["ts"]:
            restart(job, cfg)                       # a new statement, and the row is not where we left off: rescan once
            found = locate(job, cfg, row_hash)
        if found:
            top_up(job, cfg, bucket)
            fut = job["inflight"].get(row_hash)
if fut is None:
    fut = submit(job, cfg, bucket, [(row_hash, row_json)])   # anonymous record, or a row the read-ahead cannot reach
    if fut is None:                                           # answered by a request that finished meanwhile
        fut = job["inflight"].get(row_hash)
        if fut is None:
            return json.dumps(bucket[row_hash])
result = answer(bucket, row_hash, fut)
sweep(job, cfg)
return result
$py$;

COMMENT ON FUNCTION _jev_eval(text, text, text, text, text) IS
  'Internal: evaluates one row (batched with its table) against a TypeSafe question. Returns the raw answer JSON.';

-- ------------------------------------------------------------------ public API

-- Full answer JSON for a row: {"type":"noul","noul":0.93} / score / choice answers.
CREATE OR REPLACE FUNCTION jev_eval(rec anyelement, query text, kind text DEFAULT 'noul', options text[] DEFAULT NULL)
RETURNS jsonb LANGUAGE sql STABLE AS $$
  SELECT _jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, $3, to_json($4)::text)
$$;

-- Probability (0..1) that the row satisfies the natural-language condition.
CREATE OR REPLACE FUNCTION jev_prob(rec anyelement, query text)
RETURNS float8 LANGUAGE sql STABLE AS $$
  SELECT (_jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, 'noul', NULL)->>'noul')::float8
$$;

-- Boolean predicate for WHERE clauses. Threshold: argument > jev.threshold setting > 0.5.
CREATE OR REPLACE FUNCTION jev(rec anyelement, query text, threshold float8 DEFAULT NULL)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT jev_prob($1, $2) >= coalesce($3, nullif(current_setting('jev.threshold', true), '')::float8, 0.5)
$$;

-- Graded rating along ordered levels; returns the probability-weighted level index (0 .. n-1).
CREATE OR REPLACE FUNCTION jev_score(rec anyelement, query text, levels text[])
RETURNS float8 LANGUAGE sql STABLE AS $$
  SELECT (_jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, 'score', to_json($3)::text)->>'score')::float8
$$;

-- Same, normalised to 0..1 so it is comparable across rubrics.
CREATE OR REPLACE FUNCTION jev_score_norm(rec anyelement, query text, levels text[])
RETURNS float8 LANGUAGE sql STABLE AS $$
  SELECT jev_score($1, $2, $3) / greatest(array_length($3, 1) - 1, 1)
$$;

-- Classify each row into one option.
CREATE OR REPLACE FUNCTION jev_choice(rec anyelement, query text, options text[])
RETURNS text LANGUAGE sql STABLE AS $$
  SELECT _jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, 'choice', to_json($3)::text)->>'choice'
$$;

-- Confidence (0..1) of the choice / score answer.
CREATE OR REPLACE FUNCTION jev_confidence(rec anyelement, query text, kind text, options text[])
RETURNS float8 LANGUAGE sql STABLE AS $$
  SELECT (_jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, $3, to_json($4)::text)->>'confidence')::float8
$$;

-- Session statistics: requests, tokens, estimated cost, cache hits.
CREATE OR REPLACE FUNCTION jev_stats()
RETURNS jsonb LANGUAGE plpython3u STABLE AS $py$
import json
s = {"requests": 0, "input_tokens": 0, "output_tokens": 0, "rows_evaluated": 0,
     "cache_hits": 0, "api_ms": 0.0, "batches": 0, "errors": 0, "retries": 0}
jev = GD.get("jev", {})
s.update(jev.get("stats", {}))
s["estimated_cost_usd"] = round(s.get("input_tokens", 0) * 0.042 / 1_000_000, 6)
s["cached_answers"] = sum(len(b) for b in jev.get("cache", {}).values())
s["in_flight"] = sum(1 for j in jev.get("jobs", {}).values() for f in j["futures"] if not f.done())
s["connections"] = sum(len(v) for v in jev.get("conns", {}).values())   # idle, pooled
return json.dumps(s)
$py$;

-- Forget all cached judgments for this session.
CREATE OR REPLACE FUNCTION jev_cache_clear()
RETURNS void LANGUAGE plpython3u VOLATILE AS $py$
if "jev" in GD:
    GD["jev"]["cache"].clear()
    GD["jev"]["jobs"].clear()
$py$;

CREATE OR REPLACE FUNCTION jev_version() RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT '0.3.0' $$;

COMMENT ON FUNCTION jev(anyelement, text, float8) IS 'True when the row satisfies the natural-language condition (TypeSafe Jev). Usage: WHERE jev(tbl, ''condition'')';
COMMENT ON FUNCTION jev_prob(anyelement, text) IS 'Probability that the row satisfies the natural-language condition.';
COMMENT ON FUNCTION jev_score(anyelement, text, text[]) IS 'Probability-weighted rating of the row along ordered levels.';
COMMENT ON FUNCTION jev_choice(anyelement, text, text[]) IS 'Classifies the row into one of the given options.';

-- Persistent, demand-driven semantic views. Administrative APIs are owner-only.
CREATE SCHEMA jev_semantic;
REVOKE ALL ON SCHEMA jev_semantic FROM PUBLIC;

CREATE TABLE jev_semantic.sources (
    source regclass PRIMARY KEY,
    pk_column text NOT NULL,
    signature jsonb NOT NULL
);
CREATE TABLE jev_semantic.predicates (
    id bigserial PRIMARY KEY,
    source regclass NOT NULL REFERENCES jev_semantic.sources,
    name text NOT NULL,
    condition text NOT NULL,
    input_columns text[] NOT NULL,
    model text NOT NULL,
    api_url text NOT NULL,
    threshold float8 NOT NULL CHECK (threshold BETWEEN 0 AND 1),
    UNIQUE (source, name)
);
CREATE SEQUENCE jev_semantic.generation;
CREATE TABLE jev_semantic.results (
    predicate_id bigint NOT NULL REFERENCES jev_semantic.predicates ON DELETE CASCADE,
    row_id bigint NOT NULL,
    input jsonb NOT NULL,
    generation bigint NOT NULL DEFAULT nextval('jev_semantic.generation'),
    probability float8 CHECK (probability BETWEEN 0 AND 1),
    status text GENERATED ALWAYS AS
        (CASE WHEN probability IS NULL THEN 'pending' ELSE 'ready' END) STORED,
    PRIMARY KEY (predicate_id, row_id)
);
CREATE INDEX semantic_pending ON jev_semantic.results (predicate_id) WHERE probability IS NULL;
CREATE TABLE jev_semantic.views (
    view_oid regclass PRIMARY KEY,
    source regclass NOT NULL REFERENCES jev_semantic.sources,
    expression text NOT NULL,
    predicate_ids bigint[] NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('jev_semantic.sources', '');
SELECT pg_catalog.pg_extension_config_dump('jev_semantic.predicates', '');
SELECT pg_catalog.pg_extension_config_dump('jev_semantic.predicates_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('jev_semantic.generation', '');
SELECT pg_catalog.pg_extension_config_dump('jev_semantic.results', '');
SELECT pg_catalog.pg_extension_config_dump('jev_semantic.views', '');

CREATE FUNCTION public._jev_semantic_signature(rel oid) RETURNS jsonb
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT jsonb_build_object(
        'kind', c.relkind, 'persistence', c.relpersistence, 'rls', c.relrowsecurity,
        'partition', c.relispartition,
        'inherits', EXISTS (SELECT FROM pg_inherits WHERE inhrelid = rel OR inhparent = rel),
        'columns', (SELECT jsonb_agg(jsonb_build_array(a.attname, a.atttypid::regtype::text,
                     a.atttypmod, a.attnotnull, a.attgenerated) ORDER BY a.attnum)
                    FROM pg_attribute a WHERE a.attrelid = rel AND a.attnum > 0 AND NOT a.attisdropped),
        'pk_immediate', (SELECT i.indimmediate FROM pg_index i WHERE i.indrelid=rel AND i.indisprimary),
        'pk', (SELECT jsonb_agg(a.attname ORDER BY k.ord) FROM pg_index i
               CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord)
               JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
               WHERE i.indrelid = rel AND i.indisprimary AND i.indisvalid))
    FROM pg_class c WHERE c.oid = rel
$$;

CREATE FUNCTION public._jev_semantic_track() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
SET TimeZone = 'UTC' SET DateStyle = 'ISO, YMD' SET IntervalStyle = 'iso_8601'
SET extra_float_digits = 3 SET bytea_output = 'hex' AS $$
DECLARE
    pred record;
    pk text;
    fresh jsonb;
    previous jsonb;
    new_id bigint;
    old_id bigint;
BEGIN
    SELECT pk_column INTO pk FROM jev_semantic.sources WHERE source = TG_RELID;
    IF TG_OP = 'TRUNCATE' THEN
        DELETE FROM jev_semantic.results r USING jev_semantic.predicates p
        WHERE p.source = TG_RELID AND r.predicate_id = p.id;
        RETURN NULL;
    END IF;
    IF TG_OP <> 'INSERT' THEN old_id := (to_jsonb(OLD)->>pk)::bigint; END IF;
    IF TG_OP <> 'DELETE' THEN new_id := (to_jsonb(NEW)->>pk)::bigint; END IF;
    IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND old_id <> new_id) THEN
        DELETE FROM jev_semantic.results r USING jev_semantic.predicates p
        WHERE p.source = TG_RELID AND r.predicate_id = p.id AND r.row_id = old_id;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN NULL; END IF;
    FOR pred IN SELECT * FROM jev_semantic.predicates WHERE source = TG_RELID ORDER BY id LOOP
        SELECT jsonb_object_agg(k, to_jsonb(NEW)->k) INTO fresh FROM unnest(pred.input_columns) AS k;
        IF TG_OP = 'UPDATE' AND new_id = old_id THEN
            SELECT jsonb_object_agg(k, to_jsonb(OLD)->k) INTO previous FROM unnest(pred.input_columns) AS k;
            IF fresh IS NOT DISTINCT FROM previous THEN CONTINUE; END IF;
        END IF;
        INSERT INTO jev_semantic.results AS r (predicate_id, row_id, input)
        VALUES (pred.id, new_id, fresh)
        ON CONFLICT (predicate_id, row_id) DO UPDATE
        SET input = EXCLUDED.input, probability = NULL,
            generation = nextval('jev_semantic.generation');
    END LOOP;
    RETURN NULL;
END
$$;

-- A stale query may still judge its own snapshot, but can only publish to the
-- matching generation. Source writers do not take the inference advisory lock.
CREATE FUNCTION public._jev_semantic_probability(pid bigint, key bigint, rec anyelement)
RETURNS float8 LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog
SET TimeZone = 'UTC' SET DateStyle = 'ISO, YMD' SET IntervalStyle = 'iso_8601'
SET extra_float_digits = 3 SET bytea_output = 'hex' AS $$
DECLARE
    p jev_semantic.predicates;
    r jev_semantic.results;
    answer float8;
    payload jsonb;
    caller name;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed'
       OR current_setting('transaction_read_only')::boolean THEN
        RAISE EXCEPTION 'jev: semantic views require a writable READ COMMITTED transaction';
    END IF;
    SELECT * INTO STRICT p FROM jev_semantic.predicates WHERE id = pid;
    caller := CASE WHEN current_setting('role') = 'none' THEN session_user
                   ELSE current_setting('role')::name END;
    IF NOT has_table_privilege(caller, p.source, 'SELECT') THEN
        RAISE EXCEPTION 'jev: semantic view readers need SELECT on the source table';
    END IF;
    SELECT jsonb_object_agg(k, to_jsonb(rec)->k) INTO payload FROM unnest(p.input_columns) AS k;
    SELECT * INTO r FROM jev_semantic.results WHERE predicate_id = pid AND row_id = key;
    IF r.input = payload AND r.probability IS NOT NULL THEN RETURN r.probability; END IF;
    -- Reserved two-int advisory-lock namespace. Hash collisions only serialize work.
    PERFORM pg_advisory_xact_lock(1785038198, hashtext(pid::text || ':' || key::text));
    SELECT * INTO r FROM jev_semantic.results WHERE predicate_id = pid AND row_id = key;
    IF r.input = payload AND r.probability IS NOT NULL THEN RETURN r.probability; END IF;
    answer := (public._jev_eval('__jev_demand__', payload::text, p.condition, 'noul',
                json_build_object('model', p.model, 'api_url', p.api_url)::text)->>'noul')::float8;
    IF answer IS NULL OR NOT (answer BETWEEN 0 AND 1) THEN
        RAISE EXCEPTION 'jev: semantic probability must be between 0 and 1';
    END IF;
    UPDATE jev_semantic.results SET probability = answer
    WHERE predicate_id = pid AND row_id = key AND generation = r.generation AND input = payload;
    RETURN answer;
END
$$;

-- Guard tracked source structure, including RLS and disabled tracking triggers.
CREATE FUNCTION public._jev_semantic_guard_ddl() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE tracked record;
BEGIN
    IF TG_EVENT = 'sql_drop' THEN
        IF EXISTS (SELECT FROM pg_event_trigger_dropped_objects() d
                   JOIN jev_semantic.sources s ON s.source = d.objid
                   WHERE d.classid = 'pg_class'::regclass) THEN
            RAISE EXCEPTION 'jev: drop semantic predicates before dropping their source table';
        END IF;
        IF EXISTS (SELECT FROM jev_semantic.sources src WHERE
                   (SELECT count(*) FROM pg_trigger WHERE tgrelid=src.source
                    AND tgfoid='public._jev_semantic_track()'::regprocedure AND tgenabled='A') <> 2) THEN
            RAISE EXCEPTION 'jev: drop semantic predicates before dropping their tracking triggers';
        END IF;
    ELSE
        FOR tracked IN SELECT DISTINCT s.* FROM jev_semantic.sources s
                 JOIN pg_event_trigger_ddl_commands() d ON d.objid = s.source
                 WHERE d.classid = 'pg_class'::regclass LOOP
            IF public._jev_semantic_signature(tracked.source) IS DISTINCT FROM tracked.signature
               OR EXISTS (SELECT FROM pg_trigger WHERE tgrelid = tracked.source
                          AND tgfoid = 'public._jev_semantic_track()'::regprocedure
                          AND tgenabled <> 'A') THEN
                RAISE EXCEPTION 'jev: drop semantic predicates before changing source structure or tracking';
            END IF;
        END LOOP;
    END IF;
END
$$;
CREATE EVENT TRIGGER jev_semantic_ddl ON ddl_command_end
EXECUTE FUNCTION public._jev_semantic_guard_ddl();
CREATE EVENT TRIGGER jev_semantic_drop ON sql_drop
EXECUTE FUNCTION public._jev_semantic_guard_ddl();

CREATE FUNCTION public._jev_semantic_admin(action text, spec jsonb) RETURNS jsonb
LANGUAGE plpython3u VOLATILE SET search_path = pg_catalog
SET TimeZone = 'UTC' SET DateStyle = 'ISO, YMD' SET IntervalStyle = 'iso_8601'
SET extra_float_digits = 3 SET bytea_output = 'hex' AS $py$
import json, re, math
a = json.loads(spec)
def fail(message):
    plpy.error('jev: ' + message)
def run(sql, values=(), types=()):
    return plpy.execute(plpy.prepare(sql, list(types)), list(values))
def ident(s):
    return plpy.quote_ident(s)
def literal(s):
    return plpy.quote_literal(s)
def projection(cols):
    return 'pg_catalog.jsonb_build_object(' + ', '.join(
        literal(c) + ', t.' + ident(c) for c in cols) + ')'
def relation(oid):
    r = run('SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1',
            [oid], ['oid'])
    if not r:
        fail('source relation no longer exists')
    return ident(r[0]['nspname']) + '.' + ident(r[0]['relname'])
def view_name(value):
    if not isinstance(value, str): fail('view name is required')
    parts = run('SELECT parse_ident($1) AS p', [value], ['text'])[0]['p']
    if len(parts) == 1: parts.insert(0, 'public')
    if len(parts) != 2 or any(len(x.encode()) > 63 for x in parts):
        fail('use a view name or schema.view name of at most 63 bytes per identifier')
    if parts[0].startswith('pg_') or parts[0] in ('information_schema', 'jev_semantic'):
        fail('semantic views must use an application schema')
    return '.'.join(ident(x) for x in parts)
def predicate():
    rows = run('SELECT *, source::oid AS source FROM jev_semantic.predicates WHERE source=$1 AND name=$2',
               [a['source'], a['name']], ['oid', 'text'])
    if not rows: fail('unknown semantic predicate')
    return rows[0]
if not run('SELECT rolsuper FROM pg_roles WHERE rolname=current_user')[0]['rolsuper']:
    fail('semantic administration requires a superuser')

if action == 'create_predicate':
    source, name, condition, cols = a['source'], a['name'], a['condition'], a['columns']
    if not isinstance(name, str) or not re.fullmatch('[a-z_][a-z_0-9]*', name) or len(name.encode()) > 63:
        fail('predicate names must be lowercase SQL identifiers of at most 63 bytes')
    if name.upper() in ('AND', 'OR', 'NOT', 'IS', 'NULL', 'TRUE', 'FALSE'):
        fail('predicate name is reserved')
    if not isinstance(condition, str) or not condition.strip(): fail('condition must not be empty')
    if not isinstance(cols, list) or not cols or any(not isinstance(c, str) for c in cols) or len(set(cols)) != len(cols):
        fail('input columns must be a nonempty array of distinct column names')
    threshold = a['threshold']
    if threshold is None or not math.isfinite(float(threshold)) or not 0 <= float(threshold) <= 1:
        fail('threshold must be between 0 and 1')
    rel = relation(source)
    run('LOCK TABLE ONLY ' + rel + ' IN SHARE ROW EXCLUSIVE MODE')
    info = run('SELECT c.*, n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1',
               [source], ['oid'])[0]
    signature = run('SELECT public._jev_semantic_signature($1) AS s', [source], ['oid'])[0]['s']
    sig = json.loads(signature)
    if (info['relkind'] != 'r' or info['relpersistence'] != 'p' or info['relrowsecurity'] or
        sig['partition'] or sig['inherits'] or info['nspname'].startswith('pg_') or
        info['nspname'] in ('information_schema', 'jev_semantic')):
        fail('semantic sources must be ordinary permanent application tables without RLS or inheritance')
    attrs = run('SELECT attname, attnum, atttypid FROM pg_attribute WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped',
                [source], ['oid'])
    names = {c['attname'] for c in attrs}
    if name in names: fail('predicate name conflicts with a source column')
    if any(c not in names for c in cols): fail('unknown input column')
    if len(cols) > 50: fail('semantic predicates support at most 50 input columns')
    pk = sig['pk']
    key = [c for c in attrs if pk and len(pk) == 1 and c['attname'] == pk[0] and c['atttypid'] in (20, 21, 23)]
    if not key: fail('semantic sources need a single smallint, integer or bigint primary key')
    if not run('SELECT FROM pg_index WHERE indrelid=$1 AND indisprimary AND indimmediate', [source], ['oid']):
        fail('semantic primary keys must not be deferrable')
    pk = key[0]['attname']
    model = a.get('model') or run("SELECT coalesce(nullif(current_setting('jev.model', true), ''), 'jev-latest') AS m")[0]['m']
    if not isinstance(model, str) or not model.strip(): fail('model must not be empty')
    exists = run('SELECT FROM jev_semantic.sources WHERE source=$1', [source], ['oid'])
    if not exists:
        # Register only after both always-enabled triggers are installed, so the DDL guard sees a complete source.
        run('CREATE TRIGGER jev_semantic_rows AFTER INSERT OR UPDATE OR DELETE ON ' + rel +
            ' FOR EACH ROW EXECUTE FUNCTION public._jev_semantic_track()')
        run('CREATE TRIGGER jev_semantic_truncate AFTER TRUNCATE ON ' + rel +
            ' FOR EACH STATEMENT EXECUTE FUNCTION public._jev_semantic_track()')
        run('ALTER TABLE ' + rel + ' ENABLE ALWAYS TRIGGER jev_semantic_rows')
        run('ALTER TABLE ' + rel + ' ENABLE ALWAYS TRIGGER jev_semantic_truncate')
        run('INSERT INTO jev_semantic.sources VALUES ($1,$2,$3)', [source, pk, signature], ['oid','text','jsonb'])
    api_url = run("SELECT coalesce(nullif(current_setting('jev.api_url', true), ''), 'https://api.typesafe.ai/v1/systemone') AS u")[0]['u']
    pid = run('''INSERT INTO jev_semantic.predicates (source,name,condition,input_columns,model,threshold,api_url)
                 VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id''',
              [source,name,condition,cols,model,threshold,api_url], ['oid','text','text','text[]','text','float8','text'])[0]['id']
    run('INSERT INTO jev_semantic.results (predicate_id,row_id,input) SELECT $1,t.' + ident(pk) + ',' +
        projection(cols) + ' FROM ONLY ' + rel + ' t', [pid], ['bigint'])
    return json.dumps(pid)

if action == 'threshold':
    p = predicate()
    t = a['threshold']
    if t is None or not math.isfinite(float(t)) or not 0 <= float(t) <= 1:
        fail('threshold must be between 0 and 1')
    run('UPDATE jev_semantic.predicates SET threshold=$1 WHERE id=$2', [t,p['id']], ['float8','bigint'])
    return 'null'

if action == 'create_view':
    source, expression = a['source'], a['expression']
    if not isinstance(expression, str) or not expression.strip() or len(expression) > 4096:
        fail('expression must contain 1 to 4096 characters')
    name, rel = view_name(a['name']), relation(source)
    run('LOCK TABLE ONLY ' + rel + ' IN SHARE ROW EXCLUSIVE MODE')
    src = run('SELECT * FROM jev_semantic.sources WHERE source=$1', [source], ['oid'])
    if not src: fail('register a predicate on the source first')
    predicates = {p['name']: p for p in run('SELECT * FROM jev_semantic.predicates WHERE source=$1', [source], ['oid'])}
    columns = {r['attname'] for r in run('SELECT attname FROM pg_attribute WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped',
                                      [source], ['oid'])}
    # Only identifiers, literals, comparisons and Boolean operators are accepted.
    token_re = re.compile(r'''\s*(?:("(?:[^"]|"")*")|('(?:[^']|'')*')|([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)|([A-Za-z_][A-Za-z_0-9]*)|(<=|>=|<>|!=|[=<>])|([()]))''')
    tokens, pos = [], 0
    while pos < len(expression):
        if not expression[pos:].strip(): break
        m = token_re.match(expression, pos)
        if not m: fail('unsupported expression syntax at character %d' % (pos + 1))
        group = next(i for i in range(1, 7) if m.group(i) is not None)
        tokens.append((group, m.group(group)))
        pos = m.end()
    index, used = 0, set()
    def peek(word):
        return index < len(tokens) and tokens[index][0] in (4,6) and tokens[index][1].upper() == word
    def take(word):
        nonlocal index
        if peek(word): index += 1; return True
        return False
    def parse_atom():
        nonlocal index
        if take('NOT'): return ('not', parse_atom())
        if take('('):
            node = parse_or()
            if not take(')'): fail('expected closing parenthesis')
            return node
        if index >= len(tokens) or tokens[index][0] not in (1,4): fail('expected predicate or source column')
        group, value = tokens[index]; index += 1
        value = value[1:-1].replace('""','"') if group == 1 else value.lower()
        if value in predicates:
            p = predicates[value]; used.add(p['id'])
            sql = '(public._jev_semantic_probability(%d, t.%s::bigint, t) >= ' % (
                p['id'], ident(src[0]['pk_column']))
            sql += '(SELECT threshold FROM jev_semantic.predicates WHERE id=%d))' % p['id']
            return ('leaf', sql)
        if value not in columns: fail('unknown predicate or source column: ' + value)
        col = 't.' + ident(value)
        if take('IS'):
            negate = take('NOT')
            if not take('NULL'): fail('only IS NULL and IS NOT NULL are supported')
            return ('leaf', '(' + col + (' IS NOT NULL)' if negate else ' IS NULL)'))
        if index >= len(tokens) or tokens[index][0] != 5: fail('source columns require a comparison or IS NULL')
        op = tokens[index][1]; index += 1
        if index >= len(tokens): fail('expected comparison literal')
        group, value = tokens[index]; index += 1
        if group == 2: value = literal(value[1:-1].replace("''", "'"))
        elif group == 3: pass
        elif group == 4 and value.upper() in ('TRUE','FALSE','NULL'): value = value.upper()
        else: fail('comparisons require a string, number, boolean or NULL literal')
        return ('leaf', '(' + col + ' ' + op + ' ' + value + ')')
    def parse_and():
        node = parse_atom()
        while take('AND'): node = ('and', node, parse_atom())
        return node
    def parse_or():
        node = parse_and()
        while take('OR'): node = ('or', node, parse_and())
        return node
    def compile_node(node, want=True):
        # Compile "is true" / "is false", rather than computing an intermediate
        # nullable Boolean. This also skips X in WHERE NULL AND X, while NOT
        # correctly asks whether its operand is false (not merely non-true).
        kind = node[0]
        if kind == 'leaf': return '(' + node[1] + (' IS TRUE)' if want else ' IS FALSE)')
        if kind == 'not': return compile_node(node[1], not want)
        left, right = compile_node(node[1], want), compile_node(node[2], want)
        if (kind == 'and') == want:
            return '(CASE WHEN %s THEN %s ELSE FALSE END)' % (left, right)
        return '(CASE WHEN %s THEN TRUE ELSE %s END)' % (left, right)
    if len(tokens) > 128: fail('expression is limited to 128 tokens')
    tree = parse_or()
    if index != len(tokens): fail('unexpected expression token: ' + tokens[index][1])
    if not used: fail('a semantic view must reference a semantic predicate')
    # SECURITY BARRIER prevents an outer filter being pushed into the view and
    # invoking inference outside this explicitly compiled expression.
    run('CREATE VIEW ' + name + ' WITH (security_barrier=true) AS SELECT t.* FROM ONLY ' + rel +
        ' t WHERE ' + compile_node(tree))
    oid = run('SELECT $1::regclass::oid AS id', [name], ['text'])[0]['id']
    run('INSERT INTO jev_semantic.views VALUES ($1,$2,$3,$4)', [oid,source,expression,sorted(used)],
        ['oid','oid','text','bigint[]'])
    return 'null'

if action in ('drop_view', 'status'):
    name = view_name(a['name'])
    rows = run('SELECT *, view_oid::oid AS view_oid, source::oid AS source FROM jev_semantic.views WHERE view_oid=to_regclass($1)', [name], ['text'])
    if not rows: fail('unknown semantic view')
    v = rows[0]
    if action == 'drop_view':
        run('DROP VIEW ' + name)
        run('DELETE FROM jev_semantic.views WHERE view_oid=$1', [v['view_oid']], ['oid'])
        return 'null'
    counts = run('''SELECT p.name, count(r.row_id) FILTER (WHERE r.probability IS NULL) AS pending,
                    count(r.row_id) FILTER (WHERE r.probability IS NOT NULL) AS ready
                    FROM jev_semantic.predicates p LEFT JOIN jev_semantic.results r ON r.predicate_id=p.id
                    WHERE p.id=ANY($1) GROUP BY p.id ORDER BY p.name''', [v['predicate_ids']], ['bigint[]'])
    return json.dumps([dict(r) for r in counts])

if action == 'drop_predicate':
    p = predicate()
    run('LOCK TABLE ONLY ' + relation(p['source']) + ' IN SHARE ROW EXCLUSIVE MODE')
    run('DELETE FROM jev_semantic.views v WHERE NOT EXISTS (SELECT FROM pg_class c WHERE c.oid=v.view_oid)')
    if run('SELECT FROM jev_semantic.views WHERE $1=ANY(predicate_ids)', [p['id']], ['bigint']):
        fail('drop dependent semantic views before dropping the predicate')
    run('DELETE FROM jev_semantic.predicates WHERE id=$1', [p['id']], ['bigint'])
    if not run('SELECT FROM jev_semantic.predicates WHERE source=$1', [p['source']], ['oid']):
        run('DELETE FROM jev_semantic.sources WHERE source=$1', [p['source']], ['oid'])
        run('DROP TRIGGER jev_semantic_rows ON ' + relation(p['source']))
        run('DROP TRIGGER jev_semantic_truncate ON ' + relation(p['source']))
    return 'null'
fail('unknown semantic administrative operation')
$py$;

CREATE FUNCTION public.create_semantic_predicate(name text, source regclass, condition text,
    columns text[], threshold float8 DEFAULT 0.5, model text DEFAULT NULL)
RETURNS bigint LANGUAGE sql VOLATILE AS $$
    SELECT public._jev_semantic_admin('create_predicate', jsonb_build_object(
        'name',$1,'source',$2::oid,'condition',$3,'columns',$4,'threshold',$5,'model',$6))::bigint
$$;
CREATE FUNCTION public.create_semantic_view(name text, source regclass, expression text)
RETURNS void LANGUAGE sql VOLATILE AS $$
    SELECT public._jev_semantic_admin('create_view', jsonb_build_object('name',$1,'source',$2::oid,'expression',$3))
$$;
CREATE FUNCTION public.set_semantic_threshold(name text, source regclass, threshold float8)
RETURNS void LANGUAGE sql VOLATILE AS $$
    SELECT public._jev_semantic_admin('threshold', jsonb_build_object('name',$1,'source',$2::oid,'threshold',$3))
$$;
CREATE FUNCTION public.drop_semantic_view(name text)
RETURNS void LANGUAGE sql VOLATILE AS $$
    SELECT public._jev_semantic_admin('drop_view', jsonb_build_object('name',$1))
$$;
CREATE FUNCTION public.drop_semantic_predicate(name text, source regclass)
RETURNS void LANGUAGE sql VOLATILE AS $$
    SELECT public._jev_semantic_admin('drop_predicate', jsonb_build_object('name',$1,'source',$2::oid))
$$;
CREATE FUNCTION public.semantic_view_status(name text)
RETURNS jsonb LANGUAGE sql VOLATILE AS $$
    SELECT public._jev_semantic_admin('status', jsonb_build_object('name',$1))
$$;

REVOKE ALL ON FUNCTION public._jev_semantic_admin(text,jsonb),
    public._jev_semantic_signature(oid), public._jev_semantic_track(), public._jev_semantic_guard_ddl(),
    public.create_semantic_predicate(text,regclass,text,text[],float8,text),
    public.create_semantic_view(text,regclass,text), public.set_semantic_threshold(text,regclass,float8),
    public.drop_semantic_view(text), public.drop_semantic_predicate(text,regclass),
    public.semantic_view_status(text) FROM PUBLIC;
COMMENT ON FUNCTION public.create_semantic_view(text,regclass,text) IS
    'Create a lazy semantic view: only demanded predicates are evaluated; probabilities persist across sessions.';
