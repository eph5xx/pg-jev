\set VERBOSITY terse
SET jev.api_url = 'http://127.0.0.1:8765/v1/systemone';
SET jev.api_key = 'test-key';
SET jev.notices = off;
CREATE TABLE semantic_edges (id int PRIMARY KEY, body text, flag boolean);
INSERT INTO semantic_edges VALUES (1,'good',true),(2,'bad',false),(3,'bad',NULL);
SELECT create_semantic_predicate('positive','semantic_edges','good',ARRAY['body']) > 0;
SELECT create_semantic_view('edge_view','semantic_edges','NOT (flag = TRUE OR positive)');
SELECT id FROM edge_view ORDER BY id;
-- A null left operand of OR cannot be false, so NOT can skip the semantic leaf.
SELECT semantic_view_status('edge_view');
SELECT create_semantic_view('edge_all','semantic_edges','positive');
-- Spend guards still apply, and no partial results survive a failed read.
SET jev.max_rows_per_statement=1;
SELECT id FROM edge_all ORDER BY id;
RESET jev.max_rows_per_statement;
SELECT semantic_view_status('edge_all');
SELECT id FROM edge_all ORDER BY id;
-- Readers cannot redirect shared inference to a different endpoint or model.
UPDATE semantic_edges SET body='good now' WHERE id=2;
SET jev.api_url='http://127.0.0.1:1/not-the-registered-endpoint';
SET jev.model='not-the-registered-model';
SELECT id FROM edge_all ORDER BY id;
RESET jev.api_url;
RESET jev.model;
DROP TRIGGER jev_semantic_rows ON semantic_edges;
-- Data changes made in a rolled-back transaction also roll back invalidation.
BEGIN;
UPDATE semantic_edges SET body='bad' WHERE id=1;
ROLLBACK;
SELECT semantic_view_status('edge_all');
-- Standard DROP VIEW is supported; cleanup ignores its obsolete metadata.
DROP VIEW edge_view;
SELECT drop_semantic_view('edge_all');
SELECT drop_semantic_predicate('positive','semantic_edges');
-- Recreating a definition uses a new identity and fresh pending state.
SET jev.api_url='http://127.0.0.1:8765/v1/systemone';
SELECT create_semantic_predicate('positive','semantic_edges','bad',ARRAY['body']) > 0;
SELECT create_semantic_view('edge_new','semantic_edges','positive');
SELECT semantic_view_status('edge_new');
SELECT id FROM edge_new ORDER BY id;
SELECT drop_semantic_view('edge_new');
SELECT drop_semantic_predicate('positive','semantic_edges');
DROP TABLE semantic_edges;
-- Input serialization is stable across sessions' formatting settings.
CREATE TABLE semantic_dates (id int PRIMARY KEY, at timestamptz);
SET TIME ZONE 'America/New_York';
INSERT INTO semantic_dates VALUES (1,'2026-01-01 00:00:00+00');
SELECT create_semantic_predicate('recent','semantic_dates','in 2026',ARRAY['at']) > 0;
SELECT create_semantic_view('date_view','semantic_dates','recent');
SET TIME ZONE 'Asia/Tokyo';
SELECT id FROM date_view;
SET jev.api_key='';
SET TIME ZONE 'America/Los_Angeles';
SELECT id FROM date_view;
SELECT semantic_view_status('date_view');
SELECT drop_semantic_view('date_view');
SELECT drop_semantic_predicate('recent','semantic_dates');
DROP TABLE semantic_dates;
