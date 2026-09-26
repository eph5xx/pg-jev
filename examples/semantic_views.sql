-- Run on PostgreSQL 14-17 after installing jev 0.3.0.
-- Configure jev.api_key or the server's TYPESAFE_API_KEY first.
-- Only reads that demand pending predicates call the API.
CREATE EXTENSION IF NOT EXISTS jev CASCADE;

CREATE TABLE semantic_demo (
    id bigint PRIMARY KEY,
    review_text text,
    reviews integer
);
INSERT INTO semantic_demo VALUES
    (1, 'Excellent quality. I would buy this again.', 20),
    (2, 'Broke immediately. Very disappointing.', 15),
    (3, 'Works well and looks great.', 2);

SELECT create_semantic_predicate(
    name => 'positive', source => 'semantic_demo',
    condition => 'The review expresses a positive opinion',
    columns => ARRAY['review_text'], threshold => 0.8
);
SELECT create_semantic_view(
    name => 'popular_positive', source => 'semantic_demo',
    expression => 'reviews > 10 AND positive'
);

SELECT semantic_view_status('popular_positive'); -- three pending
SELECT * FROM popular_positive;                 -- evaluates only rows 1 and 2
SELECT * FROM popular_positive;                 -- persistent cache; no API calls

UPDATE semantic_demo SET reviews = 25 WHERE id = 1; -- no inference invalidation
UPDATE semantic_demo SET review_text = 'I love it now.' WHERE id = 2;
SELECT semantic_view_status('popular_positive'); -- rows 2 and 3 pending
SELECT * FROM popular_positive;                 -- evaluates only changed row 2

SELECT set_semantic_threshold('positive', 'semantic_demo', 0.9); -- reuses scores

-- Cleanup, when finished:
-- SELECT drop_semantic_view('popular_positive');
-- SELECT drop_semantic_predicate('positive', 'semantic_demo');
-- DROP TABLE semantic_demo;
