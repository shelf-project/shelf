SELECT o.o_orderstatus, n.n_name
FROM iceberg.default.orders_small o
JOIN iceberg.default.nation n ON (o.o_custkey % 25) = n.n_nationkey
ORDER BY o.o_orderkey
LIMIT 50;
