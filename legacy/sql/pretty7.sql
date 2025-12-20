SELECT
    c0,
    COUNT(c1),
    SUM(c2),
    MAX(c3),
    MIN(c4),
    AVG(c5)
FROM
    t0
WHERE
    c0 > 0
GROUP BY
    c0
HAVING
    COUNT(c1) > 100
    AND MIN(c4) >= 10
ORDER BY
    COUNT(c1) DESC,
    SUM(c2)
LIMIT 100 OFFSET 0
