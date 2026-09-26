#!/usr/bin/env python3
"""Real multi-session tests, using the mock's explicit request/release barriers."""
import json
import os
import subprocess
import time
import urllib.request

DB = 'contrib_regression'
SETTINGS = """SET jev.api_url='http://127.0.0.1:8765/v1/systemone';
SET jev.api_key='test-key'; SET jev.notices=off;
"""


def start(statement, name='semantic_test'):
    env = dict(os.environ, PGAPPNAME=name)
    return subprocess.Popen(['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-d', DB,
                             '-c', SETTINGS + statement], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def finish(process, success=True):
    out, err = process.communicate(timeout=20)
    assert (process.returncode == 0) == success, (out, err)
    return out.strip()


def sql(statement):
    return finish(start(statement))


def api(path, post=False):
    request = urllib.request.Request('http://127.0.0.1:8765/test/' + path,
                                     data=b'{}' if post else None)
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def until(check):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.02)
    raise AssertionError('timed out waiting for concurrency barrier')


def reset_input(value):
    sql("UPDATE race_source SET body='%s' WHERE id=1" % value)
    api('reset', post=True)


sql("""
CREATE TABLE race_source (id bigint PRIMARY KEY, body text);
INSERT INTO race_source VALUES (1,'good');
SELECT create_semantic_predicate('positive','race_source','test_hold good',ARRAY['body'],0.5,'test-model');
SELECT create_semantic_view('race_view','race_source','positive');
""")
try:
    # One paid request for two concurrent sessions; the waiter rechecks after commit.
    api('reset', post=True)
    first = start('SELECT id FROM race_view', 'semantic_first')
    until(lambda: len(api('state')) == 1)
    second = start('SELECT id FROM race_view', 'semantic_waiter')
    until(lambda: sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='semantic_waiter' AND wait_event='advisory'") == '1')
    api('release', post=True)
    assert finish(first) == '1'
    assert finish(second) == '1'
    requests = api('state')
    assert len(requests) == 1, requests
    assert requests[0]['rows'] == [{'body': 'good'}], requests
    assert requests[0]['model'] == 'test-model', requests

    # Source writes can commit while HTTP is in flight. The old snapshot returns
    # its own answer, but cannot publish it as the new row's probability.
    reset_input('very good')
    first = start('SELECT id FROM race_view', 'semantic_first')
    until(lambda: len(api('state')) == 1)
    sql("SET statement_timeout='2s'; UPDATE race_source SET body='bad' WHERE id=1")
    api('release', post=True)
    assert finish(first) == '1'
    assert sql("SELECT status FROM jev_semantic.results r JOIN jev_semantic.predicates p ON p.id=r.predicate_id WHERE p.name='positive' AND p.source='race_source'::regclass") == 'pending'
    assert sql('SELECT count(*) FROM race_view') == '0'
    assert len(api('state')) == 2

    # Cancellation must release transactional claims and leave pending work.
    reset_input('good again')
    first = start('SELECT id FROM race_view', 'semantic_cancel')
    until(lambda: len(api('state')) == 1)
    assert sql("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name='semantic_cancel'") == 't'
    time.sleep(0.4)  # evaluator checks cancellation at 250 ms intervals
    api('release', post=True)
    finish(first, success=False)
    assert sql("SELECT status FROM jev_semantic.results r JOIN jev_semantic.predicates p ON p.id=r.predicate_id WHERE p.source='race_source'::regclass") == 'pending'
    assert sql('SELECT id FROM race_view') == '1'
    assert len(api('state')) == 2

    # A deleted row must not be resurrected in persistent state by an old read.
    reset_input('good deleted')
    first = start('SELECT id FROM race_view', 'semantic_first')
    until(lambda: len(api('state')) == 1)
    sql("SET statement_timeout='2s'; DELETE FROM race_source")
    api('release', post=True)
    assert finish(first) == '1'
    assert sql("SELECT count(*) FROM jev_semantic.results r JOIN jev_semantic.predicates p ON p.id=r.predicate_id WHERE p.source='race_source'::regclass") == '0'
finally:
    api('release', post=True)
    sql("SELECT drop_semantic_view('race_view'); SELECT drop_semantic_predicate('positive','race_source'); DROP TABLE race_source")

print('jev: semantic concurrency tests passed')
