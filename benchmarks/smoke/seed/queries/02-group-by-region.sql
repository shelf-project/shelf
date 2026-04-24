SELECT n_regionkey, COUNT(*) AS c
FROM iceberg.default.nation
GROUP BY n_regionkey
ORDER BY n_regionkey;
