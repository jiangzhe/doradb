WITH
t0 AS (
    SELECT 1
),
t1 AS (
    SELECT 2
)
SELECT
    t0.*,
    t1.*
FROM
    t0,
    t1
