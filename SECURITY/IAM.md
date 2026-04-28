# Shelf IAM, identity, and key-rotation design

_Status: v0.1 scaffold, agent-9, 2026-04-23._
_Companion to `THREAT_MODEL.md`. This file specifies the
authentication / authorisation model; the operator (agent 8) owns
the implementation in `charts/shelf/templates/**`._

---

## 0. Design tenets

1. **Every action names its principal.** No shared "cache role".
   IRSA per tenant, STS chain for cross-tenant reads, mTLS
   SPIFFE-style SANs for in-cluster authorities.
2. **Least privilege, with specific resources.** No `s3:*`. No
   `Resource: "*"`. Every policy below lists the exact action and
   the exact ARN / pattern it applies to.
3. **Short-lived everything.** STS tokens ≤ 1 h. mTLS certs ≤ 24 h
   for operators, ≤ 90 days for service-to-service. Signed JWTs
   for tenant identity ≤ 5 min.
4. **Fail closed on auth.** An expired JWT, an unknown SPIFFE SAN,
   or a missing STS session → `PERMISSION_DENIED`. The hot path
   **falls open to direct S3 via the Trino plugin's circuit
   breaker** (BLUEPRINT §9.5) — `shelfd` itself is strictly
   fail-closed on identity.
5. **Rotation is not an event, it's a schedule.** Every key has a
   documented cadence + a runbook reference.

---

## 1. Identity topology

```
                   ┌─────────────────────────┐
                   │  AWS IAM (root of trust)│
                   └────────────┬────────────┘
                                │
       ┌───────────────────────────────────────────────┐
       │                                               │
       ▼                                               ▼
┌───────────────┐                              ┌──────────────┐
│ IRSA roles    │                              │ IRSA roles   │
│ (shelfd)      │                              │ (trainer,    │
│   per tenant  │                              │  watcher)    │
└──────┬────────┘                              └──────────────┘
       │ sts:AssumeRole (cross-tenant only)
       ▼
┌───────────────┐
│ STS session   │    ← 1 h TTL, re-assumed on expiry
└──────┬────────┘
       │
       ▼
┌───────────────┐
│ S3 data       │
└───────────────┘

Pod-level auth (in-cluster):

┌───────────────┐          mTLS+JWT         ┌────────────┐
│ Trino worker  │ ───────────────────────▶  │  shelfd    │
│ SPIFFE SAN:   │                           │  SPIFFE    │
│ spiffe://     │                           │  SAN: ...  │
│  shelf/       │                           │            │
│  tenant/<tid> │                           │            │
│  /worker      │                           │            │
└───────────────┘                           └────────────┘
```

Four identity planes, not one:

1. **AWS IAM / STS** — access to S3 (data) and S3-ConfigMap (control).
2. **Kubernetes ServiceAccount + IRSA** — pod → AWS mapping.
3. **SPIFFE-style mTLS** — in-cluster service authentication.
4. **Short-lived JWT** — per-request tenant identity, signed by the
   Trino coordinator.

Each is scoped to one job and can be rotated independently.

---

## 2. Per-component identity

### 2.1 `shelfd` → S3 (data plane reads)

**Why this is the hardest case.** A single `shelfd` pod serves
requests on behalf of multiple Trino tenants. It cannot use a single
IAM role shared across tenants, because that would let one tenant's
query read another tenant's S3 prefix via the cache.

**Design.**

- One K8s ServiceAccount per `shelfd` StatefulSet:
  `sa/shelf-data-plane` in namespace `shelf`.
- That ServiceAccount is bound via IRSA to the **base role**
  `arn:aws:iam::ACCOUNT:role/shelf-data-plane-base`.
- The base role's only permission is **`sts:AssumeRole`** on the
  per-tenant data-plane roles. It cannot directly read any S3.
- For every tenant `<tid>`, there is a per-tenant role
  `arn:aws:iam::ACCOUNT:role/shelf-tenant-<tid>` whose `trust policy`
  names the base role as the only principal allowed to assume it.
- On first request for tenant `<tid>`, `shelfd` calls
  `sts:AssumeRole` with `ExternalId = jwt.sub`, caches the credentials
  for 50 minutes, and uses them to read only that tenant's S3
  prefix.

**Base role trust policy (inline):**

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::ACCOUNT:oidc-provider/oidc.eks.REGION.amazonaws.com/id/EKS_CLUSTER_ID"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "oidc.eks.REGION.amazonaws.com/id/EKS_CLUSTER_ID:sub":
          "system:serviceaccount:shelf:shelf-data-plane"
      }
    }
  }]
}
```

**Base role permissions policy — `sts:AssumeRole` only:**

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AssumePerTenantRolesOnly",
    "Effect": "Allow",
    "Action": "sts:AssumeRole",
    "Resource": "arn:aws:iam::ACCOUNT:role/shelf-tenant-*"
  }]
}
```

Note that the resource pattern `shelf-tenant-*` is strictly scoped
to a fixed prefix + account — there is no `*` on action, region, or
account ID, and no other service in the account uses that naming
convention.

**Per-tenant role trust policy:**

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::ACCOUNT:role/shelf-data-plane-base" },
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": {
        "sts:ExternalId": "<tid>"
      }
    }
  }]
}
```

**Per-tenant role permissions policy (example — `tenant cdp-analytics`):**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadDataForThisTenant",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::example-prod-gold-layer/cdp/icesheet/silver_offline_event_data_2026/*",
        "arn:aws:s3:::example-prod-gold-layer/cdp/icesheet/silver_offline_event_data_2026/_metadata/*"
      ]
    }
  ]
}
```

Hot-path invariants:

- Only `s3:GetObject`. **No `s3:ListBucket`** on the hot path (if we
  need a listing, it is done once offline by the trainer, not by
  `shelfd`).
- **No `s3:Put*`, `s3:Delete*`, `s3:*LifecycleConfiguration`** —
  `shelfd` never mutates S3.
- **No `s3:GetBucketAcl`, `s3:GetBucketPolicy`** — no bucket-level
  queries from the hot path.
- Resource ARNs are tenant-prefix-scoped; no `arn:aws:s3:::*`.

### 2.2 `shelfd` → S3 config bucket (pin list)

Read-only, single prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "ReadShelfPinList",
    "Effect": "Allow",
    "Action": [
      "s3:GetObject",
      "s3:GetObjectVersion"
    ],
    "Resource": [
      "arn:aws:s3:::pw-shelf-config-prod/shelf/pin_list.json",
      "arn:aws:s3:::pw-shelf-config-prod/shelf/pin_list.json.sig"
    ]
  }]
}
```

Intentionally excluded: `s3:ListBucket`, `s3:ListBucketVersions`.
Intentionally excluded: any write. Pin list is immutable from
`shelfd`'s point of view.

### 2.3 Worker ↔ `shelfd` — mTLS + tenant JWT

Two-layer identity on every data-plane request:

1. **mTLS** (pod identity). Both client and server certs issued by
   cert-manager using an in-cluster CA (`shelf-ca`).
   - Worker SAN: `spiffe://shelf/replica/<rep>/tenant/<tid>/worker`
     — `<rep>` is the Trino replica (`rep-0`..`rep-3`), `<tid>`
     is the tenant resource-group.
   - `shelfd` server SAN: `spiffe://shelf/replica/<rep>/shelfd/<pod>`.
   - Cert TTL: 24 h for the first revision; drop to 4 h in Phase 3
     once cert-manager rotation stability is observed.
2. **JWT** (request identity). Carried in
   `Authorization: Bearer <jwt>` on every `GET /cache/...`.
   - Issued by the Trino coordinator pod using a private key stored
     in AWS KMS (HSM-backed) — see §3.
   - Claims: `iss` (coordinator URL), `sub` (`tenant_id`), `aud`
     (`shelf.data`), `exp` (≤ 5 min), `iat`, `query_id`, `user`.
   - `shelfd` verifies signature against the coordinator's JWKS
     URL (cached for 10 min); on verification failure, returns
     401.

The JWT's `sub` is the single source of truth for which tenant's
STS role `shelfd` assumes. This closes D-I2 (cross-tenant leakage
via cache hits).

### 2.4 Control-plane RBAC — Pin / Evict / Stats / Prefetch

gRPC on :9092. Authentication is mTLS; authorisation is role-table.

| Role                     | SAN pattern                                      | Pin | Evict | Reload | Stats (own tenant) | Stats (all tenants) | Prefetch |
| ------------------------ | ------------------------------------------------ | --- | ----- | ------ | ------------------ | ------------------- | -------- |
| `shelf-oncall`           | `spiffe://shelf/operator/oncall/*`               | yes | yes   | yes    | yes                | yes                 | no       |
| `shelf-oncall-readonly`  | `spiffe://shelf/operator/ro/*`                   | no  | no    | no     | yes                | yes                 | no       |
| `coordinator-prefetcher` | `spiffe://shelf/component/coordinator`           | no  | no    | no     | no                 | no                  | yes      |
| `snapshot-watcher`       | `spiffe://shelf/component/snapshot-watcher`      | no  | no    | no     | no                 | no                  | (separate `SnapshotUpdate` RPC only) |
| `tenant-tools`           | `spiffe://shelf/tenant/<tid>/tools`              | no  | no    | no     | yes (same `<tid>`) | no                  | no       |

Rules:

- Mapping SAN → role is a server-side allowlist, baked into the
  Helm chart (no CM mutation at runtime).
- Every RPC logs `{actor_cert_fingerprint, method, args_hash}` to
  the admin audit trail (C-R1).
- Deny-by-default: unknown SAN → `PERMISSION_DENIED`.

### 2.5 `shelfctl` (operator CLI)

- Issued an operator cert by cert-manager via a `CertificateRequest`
  signed with a hardware-backed OIDC login (rota member's YubiKey).
- Cert TTL = **24 h**; SAN = `spiffe://shelf/operator/oncall/<rota>`.
- `shelfctl` prints the cert fingerprint and expiry at every
  startup; expired cert → command fails with a pointer to the
  re-enrollment runbook.
- Admin RPCs always require double-confirmation for destructive
  operations: `shelfctl evict --confirm=<random-id-from-stats>`.

### 2.6 Trainer → S3 config bucket (write)

Strictly one action, one resource:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "PublishPinList",
    "Effect": "Allow",
    "Action": [
      "s3:PutObject",
      "s3:AbortMultipartUpload"
    ],
    "Resource": [
      "arn:aws:s3:::pw-shelf-config-prod/shelf/pin_list.json",
      "arn:aws:s3:::pw-shelf-config-prod/shelf/pin_list.json.sig"
    ],
    "Condition": {
      "StringEquals": {
        "s3:x-amz-object-lock-mode": "GOVERNANCE"
      }
    }
  }]
}
```

Intentionally excluded: `s3:DeleteObject`, `s3:DeleteObjectVersion`,
`s3:PutObjectAcl`, and any permission on other prefixes. The trainer
**cannot delete or overwrite in destructive ways** — Object
Versioning retains history, Object Lock retains immutability.

### 2.7 snapshot-watcher → HMS

- Read-only IAM role `shelf-snapshot-watcher-ro` with a single policy
  that permits `hive:GetTable`, `hive:GetPartitions`, and the
  underlying `glue:GetTable`, `glue:GetPartitions` if Glue is the
  catalog.
- No `hive:Create*`, `hive:Alter*`, `hive:Drop*`, `glue:*Write*`.
- Watcher runs as a singleton via K8s lease lock (avoids a race
  where two watchers publish conflicting snapshot maps).

---

## 3. Key rotation cadence

One table. Entries marked **P** are primary artefacts owned by the
security rota; **O** are platform-owned but we track them.

| Key / cert                                    | Type           | TTL       | Rotation cadence  | Owner | Runbook                       |
| --------------------------------------------- | -------------- | --------- | ----------------- | ----- | ----------------------------- |
| Coordinator JWT signing key (AWS KMS CMK)     | Asymmetric EC  | 1 year    | Annual + on rota change | P | `runbooks/rotate-jwt-key.md` (agent 8) |
| `shelfd` mTLS server cert                     | TLS            | 90 days   | cert-manager automatic | O | `runbooks/cert-manager.md`    |
| Trino worker mTLS client cert                 | TLS            | 24 h      | cert-manager automatic | O | `runbooks/cert-manager.md`    |
| Operator mTLS client cert (`shelfctl`)        | TLS            | 24 h      | YubiKey re-enroll + cert-manager | P | `runbooks/operator-enrol.md`  |
| In-cluster CA (`shelf-ca`)                    | Root CA        | 1 year    | Annual + emergency re-issue on compromise | P | `runbooks/rotate-shelf-ca.md` |
| Base IRSA role (`shelf-data-plane-base`)      | IAM role       | —         | On trust-policy change only | P | inline in this file           |
| Per-tenant IAM role (`shelf-tenant-<tid>`)    | IAM role       | —         | Provisioned by trainer PR; removed when tenant leaves | P | `runbooks/add-tenant.md`      |
| STS session (per-tenant, in-memory)           | STS token      | 1 h       | On expiry (automatic, 10-min early)                    | P | n/a — runtime                 |
| S3 config-bucket CMK                          | KMS symmetric  | —         | Annual (manual re-encrypt) | O | `runbooks/rotate-config-kms.md` |
| Release signing key (cosign / Sigstore OIDC)  | cosign keyless | per-run   | Per-release, anchored to GitHub OIDC | P | `SECURITY/SUPPLY_CHAIN.md §5` |
| PGP key (security contact)                    | OpenPGP        | 2 years   | Every 2 years or on primary-owner change | P | `runbooks/rotate-security-pgp.md` |
| gitleaks / trufflehog allowlist               | git config     | —         | Quarterly audit     | P | `runbooks/secret-scan-audit.md` |

Key ceremony for the **coordinator JWT signing key**:

1. `aws kms create-key --key-spec ECC_NIST_P256 --key-usage SIGN_VERIFY`
2. Alias `alias/shelf-jwt-signer-YYYY-Q` with the quarter.
3. Two rota members attest to the ceremony (logs + Slack thread);
   attestation recorded in `docs/adr/` as a dated entry.
4. Old alias kept live for a 30-day validation overlap; then deleted.
5. JWKS endpoint published by the coordinator rotates with a 10-min
   overlap.

Any missed rotation triggers a PagerDuty alert from the Sigstore /
KMS expiry monitors (owned by agent 8).

---

## 4. NetworkPolicy summary (for agent 8 to implement)

This section is **not** a NetworkPolicy manifest (agent 8 owns that
in `charts/shelf/templates/networkpolicy.yaml`). It is the
requirements the policy must enforce. CODEOWNERS routes changes to
`networkpolicy.yaml` to the security rota.

- **Ingress to `shelfd` :9090 (data plane):** allowed from Trino
  worker pods with label `app.kubernetes.io/component=worker` in the
  `trino-*` namespaces only. Deny from anywhere else.
- **Ingress to `shelfd` :9091 (metrics):** allowed from Prometheus
  scraper pod only.
- **Ingress to `shelfd` :9092 (control plane):** allowed from:
  - Trino coordinator pods (for `Prefetch`)
  - `snapshot-watcher` pod (for `SnapshotUpdate`)
  - `shelfctl` invocations via a jumphost Service (for admin ops)
- **Egress from `shelfd`:** allowed to:
  - S3 (443) — via VPC endpoint where possible
  - HMS (configured port) — for `snapshot-watcher` only
  - KMS (443) — for signature verification
  - `kubernetes.default.svc` — for ServiceAccount token refresh
  - Nothing else. No general internet egress.

---

## 5. Open items (non-blocking)

- **SPIFFE server.** We specify SPIFFE-style SANs but do not require
  a SPIFFE runtime (SPIRE) for v1. If the platform adopts SPIRE in
  Phase 3+, this doc is revised to use SPIRE directly. Tracked in
  plan §8 decisions.
- **Envelope encryption for pin-list signatures.** Currently the
  signature is detached (`pin_list.json.sig`). Consider co-signing
  with a second trainer identity to close T-R1 fully. Not blocking
  v0.5.
- **mTLS on the S3-shim (`:9093`).** v0.5 keeps the shim on a
  Service gated by NetworkPolicy (no mTLS). If the shim is ever
  exposed to non-Trino engines, we re-visit. Tracked as a Phase 6
  open item.

---

## 6. Related

- `THREAT_MODEL.md` — threats these controls mitigate.
- `SUPPLY_CHAIN.md` — how the artefacts referenced here are signed.
- `CHECKLIST.md` — pre-release verification that rotation, mTLS,
  and RBAC are wired up.
- Runbooks under `runbooks/` — owned by agent 8.
