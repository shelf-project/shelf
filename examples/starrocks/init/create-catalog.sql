-- Drop any prior incarnation so re-runs of run.sh are idempotent.
DROP CATALOG IF EXISTS iceberg_demo;

-- StarRocks Iceberg external catalog, REST metastore, MinIO storage,
-- but with `aws.s3.endpoint` pointed at shelfd's S3-compat shim
-- instead of MinIO directly. Property names verified against
-- https://docs.starrocks.io/docs/3.2/data_source/icebergtutorial/
--
--   - `iceberg.catalog.type`          = "rest"  (3.0+ supports this)
--   - `iceberg.catalog.uri`           REST server endpoint
--   - `iceberg.catalog.warehouse`     warehouse name registered with REST
--   - `aws.s3.endpoint`               *** points at shelfd:9092 ***
--   - `aws.s3.enable_path_style_access` = "true"  (shelfd shim is path-style)
--   - `aws.s3.access_key`/`secret_key` are dummy placeholders — shelfd
--     ignores SigV4 Authorization headers (signature-agnostic by design),
--     but StarRocks's S3 client still requires *some* credential set.
--   - `client.factory` tells StarRocks's Iceberg client to use the
--     access_key/secret_key path instead of the default credential
--     chain (instance profile / env / etc.).
CREATE EXTERNAL CATALOG iceberg_demo
COMMENT "Iceberg via Shelf S3 shim"
PROPERTIES (
  "type"                              = "iceberg",
  "iceberg.catalog.type"              = "rest",
  "iceberg.catalog.uri"               = "http://iceberg-rest:8181",
  "iceberg.catalog.warehouse"         = "warehouse",
  "aws.s3.endpoint"                   = "http://shelfd:9092",
  "aws.s3.enable_path_style_access"   = "true",
  "aws.s3.access_key"                 = "dummy",
  "aws.s3.secret_key"                 = "dummy",
  "aws.s3.region"                     = "us-east-1",
  "client.factory"                    = "com.starrocks.connector.iceberg.IcebergAwsClientFactory"
);

SHOW CATALOGS;
