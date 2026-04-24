# shelf-trino-plugin — configuration keys

Every key is read from the catalog `.properties` file Trino passes into the
`EventListenerFactory` and the filesystem factory. Keys match BLUEPRINT §6.2
verbatim.

| Key                        | Type          | Default                                  | Range / allowed values                                                  | Notes                                                                                                          |
|---------------------------|---------------|------------------------------------------|-------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `shelf.enabled`           | boolean       | `false`                                  | `true` / `false`                                                        | Master switch. When `false`, plugin is a pass-through to the underlying S3 filesystem.                         |
| `shelf.endpoint`          | string        | `shelf.shelf.svc.cluster.local:9090`     | `host:port` — DNS name or VIP                                           | Data-plane endpoint. Resolved every 5 s (SHELF-20); DNS TTL must be ≤ 5 s.                                      |
| `shelf.tenant`            | string        | `default`                                | Trino resource-group name, `[a-z0-9_-]+`                                 | Used for per-tenant quotas and admission. Should match the Trino replica name (e.g. `replica-2`).              |
| `shelf.fallback.on-error` | enum          | `direct-s3`                              | `direct-s3` \| `fail`                                                   | `direct-s3` = fail-open to S3 (the only sane production value). `fail` exists strictly for integration tests.  |
| `shelf.prefetch.enabled`  | boolean       | `false`                                  | `true` / `false`                                                        | Controls `ShelfPrefetchListener`. Stays off in Phase 0; turned on by SHELF-PHASE-2 after E1 confirms signal.   |
| `shelf.granularity`       | csv string    | `row-group,footer,manifest`              | subset of `row-group`, `footer`, `manifest`, `page-index`, `file`       | Which object levels the plugin is willing to route through Shelf. Everything else passes through to S3.        |
| `shelf.rpc.timeout-ms`                    | int (ms) | `200`     | `1` .. `60000`  | Per-request deadline for the hot-path `/cache/...` range-GET. Aligns with `ShelfHttpClient.DEFAULT_TIMEOUT`.                                          |
| `shelf.membership.refresh-interval-ms`    | int (ms) | `5000`    | `1` .. `300000` | `MembershipResolver` DNS-resolve + `/stats` polling cadence (BLUEPRINT §6.3). Requires JVM DNS TTL ≤ this value (`-Dsun.net.inetaddr.ttl=0` is typical). |
| `shelf.membership.stats-timeout-ms`       | int (ms) | `2000`    | `1` .. `60000`  | Per-pod `/stats` poll deadline. Runs on the resolver's background scheduler — deliberately larger than the hot-path deadline.                         |
| `shelf.footer.prefetch.kib`               | int (KiB) | `64`    | `1` .. `256`   | SHELF-15. Parquet footer prefetch window. On `newInputFile(.parquet)` the last `N` KiB are best-effort range-GET'd from the metadata pool (see `docs/design-notes/SHELF-15-footer-prefetch.md`). |

## Example catalog properties

```properties
# iceberg.properties
connector.name=iceberg
hive.metastore.uri=thrift://trino-prod-metastore.penpencil.co:9083
iceberg.catalog.type=hive_metastore

# Shelf
fs.shelf.enabled=true
shelf.endpoint=shelf.shelf.svc.cluster.local:9090
shelf.tenant=replica-2
shelf.fallback.on-error=direct-s3
shelf.prefetch.enabled=true
shelf.granularity=row-group,footer,manifest
```

## Out-of-scope keys (Phase 2+)

These are mentioned in the BLUEPRINT but not yet wired. Ticket references
are in 03-plan.md §4:

| Key                                      | Landing ticket | Notes                                                                     |
|------------------------------------------|----------------|---------------------------------------------------------------------------|
| `shelf.admission.size_threshold_mib`     | SHELF-25       | default 1024; pinned-bypass defaults `true`                               |
| `shelf.admission.pinned_bypass`          | SHELF-25       | boolean                                                                   |
| `shelf.circuit.failure-threshold`        | SHELF-11       | default 5 (BLUEPRINT §9.5)                                                |
| `shelf.circuit.open-duration-ms`         | SHELF-11       | default 10000 (BLUEPRINT §9.5)                                            |

Keys appended here must also appear in the SPI-level contracts file
(`shelf/contracts/config-keys.md`) once that file lands.
