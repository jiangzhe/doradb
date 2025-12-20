SELECT
    *
FROM
    t0
    JOIN t1
    ON
        t0.c0 = t1.c0
        AND t0.c1 = t1.c1
        AND t0.c2 = t1.c2
    LEFT JOIN t1 AS t2
    ON
        t0.c0 = t2.c1
    RIGHT JOIN t3
    ON
        t0.c0 = t3.c0
    FULL JOIN t4
    ON
        t3.c1 = t4.c1
    JOIN t5
    USING (c0, c1, c2)
