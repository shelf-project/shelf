# Trino catalog example

[`dev/cdp_shelf.properties`](dev/cdp_shelf.properties) is a working example
of a Trino Iceberg catalog routed through shelfd's S3 shim.

## Per-coordinator pinning

In a multi-coordinator Trino deployment you can pin each coordinator to
a specific shelfd pod (rather than letting the headless service round-
robin across all of them). This stabilises each coordinator's DRAM
working set across scheduler boundaries.

The simplest scheme is modular pinning by replica index. With 4
coordinators and 3 shelfd pods:

| Coordinator | shelfd target |
| ----------- | --------------- |
| `rep-0`     | `shelf-0.<release>.<ns>.svc.cluster.local:9092` |
| `rep-1`     | `shelf-1.<release>.<ns>.svc.cluster.local:9092` |
| `rep-2`     | `shelf-2.<release>.<ns>.svc.cluster.local:9092` |
| `rep-3`     | `shelf-0.<release>.<ns>.svc.cluster.local:9092` (wraps) |

Render one catalog properties file per coordinator with the pinned
endpoint, and apply it as a per-pod ConfigMap key. (HRW-aware client-
side routing is on the roadmap; pinning is a temporary scheme until
that lands.)

## Verifying

The shim ignores SigV4 headers, so the catalog can be smoke-tested end
to end with any non-empty `s3.aws-access-key` / `s3.aws-secret-key`
pair. Confirm the Iceberg metadata-cache and the shim are both warming
by reading `shelf_hits_total` and `shelf_misses_total` on the
shelfd pods after a known query.
