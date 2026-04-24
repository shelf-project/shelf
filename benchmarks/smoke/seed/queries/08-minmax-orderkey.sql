SELECT MIN(o_orderkey) AS lo, MAX(o_orderkey) AS hi
FROM iceberg.default.orders_small;
