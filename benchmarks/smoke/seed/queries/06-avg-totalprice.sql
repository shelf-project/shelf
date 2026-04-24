SELECT round(AVG(o_totalprice), 2) AS avg_price
FROM iceberg.default.orders_small;
