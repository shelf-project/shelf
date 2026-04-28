SELECT o_orderstatus, COUNT(*) AS c
FROM iceberg.default.orders_small
GROUP BY o_orderstatus
ORDER BY o_orderstatus;
