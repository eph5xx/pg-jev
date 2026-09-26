#!/usr/bin/env python3
"""Deterministic stand-in for https://api.typesafe.ai/v1/systemone, used by the regression tests.

Rules (so expected output is stable):
  noul   -> 0.9 if the LAST word of `state.condition` appears (case-insensitively) in the row JSON, else 0.1
  score  -> level index = length of the row JSON modulo number of levels (one-hot probabilities)
  choice -> option index  = length of the row JSON modulo number of options
  A condition containing "trigger422" returns HTTP 422 (non-retryable error path).
  usage.input_tokens = len(request body) // 4
Run: python3 test/mock_api.py [port]   (default 8765)
"""
import json, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TEST_LOCK = threading.Lock()
TEST_REQUESTS = []
TEST_GATES = {}

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"   # keep-alive, so the extension's connection reuse is exercised

    def log_message(self, *a):  # quiet
        pass

    def do_GET(self):
        if self.path != '/test/state':
            return self._send(404, {})
        with TEST_LOCK:
            state = list(TEST_REQUESTS)
        self._send(200, state)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == '/test/reset':
            with TEST_LOCK:
                for gate in TEST_GATES.values():
                    gate.set()
                TEST_REQUESTS.clear()
                TEST_GATES.clear()
            return self._send(200, {})
        if self.path == '/test/release':
            with TEST_LOCK:
                for gate in TEST_GATES.values():
                    gate.set()
            return self._send(200, {})
        if self.headers.get("Authorization", "") != "Bearer test-key":
            return self._send(401, {"error": "invalid api key"})
        req = json.loads(body)
        state, questions = req["state"], req["questions"]
        rows = state.get("rows", [])
        cond = state.get("condition", "")
        with TEST_LOCK:
            TEST_REQUESTS.append({'condition': cond, 'rows': rows, 'model': req['model']})
            gate = TEST_GATES.setdefault(cond, threading.Event()) if 'test_hold' in cond else None
        if gate is not None:
            gate.wait(timeout=15)
        if "trigger422" in cond:
            return self._send(422, {"error": "mock validation failure"})
        needle = cond.split()[-1].lower() if cond.split() else ""
        answers = {}
        for qid, q in questions.items():
            i = int(qid[1:])
            row_json = json.dumps(rows[i], sort_keys=True)
            if q["type"] == "noul":
                answers[qid] = {"type": "noul", "noul": 0.9 if needle and needle in row_json.lower() else 0.1}
            elif q["type"] == "score":
                levels = q["criteria"]; k = len(row_json) % len(levels)
                answers[qid] = {"type": "score", "score": float(k),
                                "legend": {str(j): l for j, l in enumerate(levels)},
                                "probabilities": {str(j): (1.0 if j == k else 0.0) for j in range(len(levels))},
                                "confidence": 1.0}
            elif q["type"] == "choice":
                opts = list(q["criteria"].keys()); k = len(row_json) % len(opts)
                answers[qid] = {"type": "choice", "choice": opts[k],
                                "probabilities": {o: (1.0 if j == k else 0.0) for j, o in enumerate(opts)},
                                "confidence": 1.0}
        self._send(200, {"model": "jev-mock", "answers": answers,
                         "usage": {"input_tokens": len(body) // 4, "output_tokens": len(answers)}})

    def _send(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass  # cancellation tests deliberately abandon an in-flight request

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
