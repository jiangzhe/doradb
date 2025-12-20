WITH RECURSIVE
t0 AS (
    SELECT 1 AS n
    UNION ALL
    SELECT
        n + 1
    FROM
        t0
    WHERE
        n < 5
)
SELECT
    *
FROM
    t0
