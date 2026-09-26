-- Lazy predicates: all inference uses the deterministic mock.
\set VERBOSITY terse
SET jev.api_url = 'http://127.0.0.1:8765/v1/systemone';
SET jev.api_key = 'test-key';
SET jev.notices = off;
CREATE TABLE products (id bigint PRIMARY KEY, review_text text, category text, reviews int);
INSERT INTO products VALUES (1,'good','green',20), (2,'bad','red',20),
                           (3,'good','green',5), (4,NULL,'green',NULL);
SELECT create_semantic_predicate('positive','products','is good',ARRAY['review_text'],0.8) > 0 AS registered;
SELECT create_semantic_predicate('green','products','is green',ARRAY['category']) > 0 AS registered;
SELECT create_semantic_predicate('broken','products','trigger422',ARRAY['review_text']) > 0 AS registered;
SELECT create_semantic_view('popular_positive','products','reviews > 10 AND positive');
SELECT semantic_view_status('popular_positive');
SELECT (jev_stats()->>'requests')::int AS no_registration_inference;
SELECT id FROM popular_positive ORDER BY id;
SELECT semantic_view_status('popular_positive');
SELECT (jev_stats()->>'requests')::int AS only_two_needed;
-- Even NULL AND positive cannot match: the first read skips that inference.
SELECT create_semantic_view('positive_again','products','positive');
SELECT id FROM positive_again ORDER BY id;
SELECT (jev_stats()->>'requests')::int AS shared_results;
SELECT create_semantic_view('complex_products','products','(positive AND green) OR reviews > 30');
SELECT id FROM complex_products ORDER BY id;
SELECT semantic_view_status('complex_products');
-- Changing thresholds is free, across all views.
SELECT set_semantic_threshold('positive','products',0.95);
SELECT count(*) FROM positive_again;
SELECT set_semantic_threshold('positive','products',0.8);
-- Unrelated and no-op updates preserve results; only relevant inputs invalidate.
UPDATE products SET reviews=21 WHERE id=1;
UPDATE products SET review_text=review_text;
SELECT semantic_view_status('complex_products');
UPDATE products SET review_text='good now' WHERE id=2;
SELECT semantic_view_status('complex_products');
SELECT id FROM complex_products ORDER BY id;
SELECT semantic_view_status('complex_products');
-- OR and AND skip the deliberately failing predicate.
SELECT create_semantic_view('or_skip','products','reviews IS NOT NULL OR broken');
SELECT id FROM or_skip WHERE id=1;
SELECT create_semantic_view('and_skip','products','reviews < 0 AND broken');
SELECT count(*) FROM and_skip;
-- NULL is preserved through NOT; the selected predicate must still be resolved.
SELECT create_semantic_view('null_logic','products','NOT (reviews > 0 AND positive)');
SELECT id FROM null_logic ORDER BY id;
SELECT create_semantic_view('fails','products','broken');
SELECT * FROM fails;
SELECT semantic_view_status('fails');
-- Rollback discards probabilities; a later read really evaluates again.
INSERT INTO products VALUES (5,'good','red',20);
BEGIN;
SELECT id FROM positive_again WHERE id=5;
ROLLBACK;
SELECT status FROM jev_semantic.results r JOIN jev_semantic.predicates p ON p.id=r.predicate_id
WHERE p.name='positive' AND r.row_id=5;
SELECT id FROM positive_again WHERE id=5;
-- Primary-key changes, deletes and truncate clean up state.
UPDATE products SET id=50 WHERE id=5;
SELECT count(*) AS old_key_gone FROM jev_semantic.results WHERE row_id=5;
DELETE FROM products WHERE id=50;
SELECT count(*) AS deleted_key_gone FROM jev_semantic.results WHERE row_id=50;
-- API key is unnecessary for a warm read, including in a fresh session.
\connect -reuse-previous=on
SET jev.api_key = '';
SELECT id FROM positive_again ORDER BY id;
-- Unsupported syntax and source changes are rejected transactionally.
SELECT create_semantic_view('invalid','products','positive; DROP TABLE products');
SELECT create_semantic_view('invalid','products','positive OR pg_sleep(1)');
SELECT create_semantic_view('invalid','products','missing');
SELECT create_semantic_predicate('reviews','products','good',ARRAY['review_text']);
SELECT create_semantic_predicate('invalid','products','good',ARRAY['missing']);
SELECT create_semantic_predicate('invalid','products','good',ARRAY['review_text'],2);
CREATE TABLE no_key (id int, txt text);
SELECT create_semantic_predicate('invalid','no_key','good',ARRAY['txt']);
DROP TABLE no_key;
ALTER TABLE products RENAME COLUMN review_text TO renamed;
ALTER TABLE products ENABLE ROW LEVEL SECURITY;
ALTER TABLE products DISABLE TRIGGER jev_semantic_rows;
DROP TABLE products CASCADE;
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT * FROM positive_again;
ROLLBACK;
BEGIN READ ONLY;
SELECT * FROM positive_again;
ROLLBACK;
-- Ordinary readers need explicit source access; private state is not exposed.
CREATE ROLE semantic_reader;
GRANT SELECT ON positive_again TO semantic_reader;
SET ROLE semantic_reader;
SELECT count(*) FROM positive_again;
RESET ROLE;
GRANT SELECT ON products TO semantic_reader;
SET ROLE semantic_reader;
SELECT count(*) FROM positive_again;
SELECT * FROM jev_semantic.results;
SELECT create_semantic_view('unauthorized','products','positive');
RESET ROLE;
REVOKE ALL ON products,positive_again FROM semantic_reader;
DROP ROLE semantic_reader;
SELECT drop_semantic_predicate('positive','products');
SELECT drop_semantic_view('popular_positive');
SELECT drop_semantic_view('positive_again');
SELECT drop_semantic_view('complex_products');
SELECT drop_semantic_view('or_skip');
SELECT drop_semantic_view('and_skip');
SELECT drop_semantic_view('null_logic');
SELECT drop_semantic_view('fails');
TRUNCATE products;
SELECT count(*) AS truncated_results FROM jev_semantic.results;
SELECT drop_semantic_predicate('positive','products');
SELECT drop_semantic_predicate('green','products');
SELECT drop_semantic_predicate('broken','products');
DROP TABLE products;
