SELECT n.n_name, r.r_name
FROM iceberg.default.nation n
JOIN iceberg.default.region r ON n.n_regionkey = r.r_regionkey
ORDER BY n.n_name;
