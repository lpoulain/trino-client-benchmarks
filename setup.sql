DROP TABLE memory.default.orders;

CREATE TABLE memory.default.orders AS
SELECT *, cast('2001-08-22 01:23:45.123456789 -08:00' AS TIMESTAMP(9) WITH TIME ZONE) AS col_ts, cast('01:23:45.123456' AS TIME(6)) AS col_time, cast('Infinity' AS REAL) AS col_inf, cast(4.9E-324 AS DOUBLE) AS col_double, 1 AS col_int
FROM tpch.sf100.orders
LIMIT 1000000;

