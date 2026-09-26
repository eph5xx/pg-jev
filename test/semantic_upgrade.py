#!/usr/bin/env python3
"""Upgrade, backup/restore and extension lifecycle in isolated databases."""
import subprocess
import tempfile


def command(*args, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, **kwargs)
    assert result.returncode == 0, (args, result.stderr)
    return result.stdout.strip()


def sql(db, statement):
    return command('psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', '-d', db, '-c', statement)


db = 'jev_upgrade_test'
restored = 'jev_restore_test'
command('createdb', db)
try:
    assert sql(db, "CREATE EXTENSION jev VERSION '0.2.0' CASCADE; SELECT jev_version()") == '0.2.0'
    assert sql(db, "ALTER EXTENSION jev UPDATE TO '0.3.0'; SELECT jev_version()") == '0.3.0'
    sql(db, """
    SET jev.api_url='http://127.0.0.1:8765/v1/systemone'; SET jev.api_key='test-key'; SET jev.notices=off;
    CREATE TABLE upgrade_source (id int PRIMARY KEY, removed text, body text);
    ALTER TABLE upgrade_source DROP COLUMN removed;
    INSERT INTO upgrade_source VALUES (1,'good');
    SELECT create_semantic_predicate('positive','upgrade_source','good',ARRAY['body']);
    SELECT create_semantic_view('upgrade_view','upgrade_source','positive');
    SELECT * FROM upgrade_view;
    """)
    assert sql(db, 'SELECT id FROM upgrade_view') == '1'
    command('createdb', restored)
    try:
        dump = command('pg_dump', '--no-owner', '--no-privileges', db)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql') as output:
            output.write(dump)
            output.flush()
            command('psql', '-Xq', '-v', 'ON_ERROR_STOP=1', '-d', restored, '-f', output.name)
        assert sql(restored, 'SELECT id FROM upgrade_view') == '1'
        sql(restored, 'DROP EXTENSION jev CASCADE; DROP TABLE upgrade_source')
    finally:
        command('dropdb', restored)
finally:
    command('dropdb', db)
print('jev: semantic upgrade and restore tests passed')
