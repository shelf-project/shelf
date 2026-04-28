# `infra/penpencil/` — origin-cluster overlay

This directory holds **per-cluster overlays for the origin penpencil cluster
that birthed Shelf.** It is **not** part of the OSS distribution: it carries
bucket names, IRSA role ARNs, in-cluster service hostnames, dashboards
folder UIDs, and other site-specific values that only make sense inside the
penpencil EKS environment.

The OSS-clean equivalents live at the canonical paths in this repo
(`charts/shelf/values.yaml`, `observability/dashboards/*` once a generic
dashboard ships, etc.). This directory exists so the origin cluster keeps
running off the same git source while we develop in the open.

## Layout

```
infra/penpencil/
├── charts/shelf/
│   ├── values.yaml          # snapshot of charts/shelf/values.yaml *before*
│   │                        # the OSS-clean defaults landed; serves as the
│   │                        # baseline overlay for the origin cluster
│   ├── values-staging.yaml  # was charts/shelf/values-staging.yaml
│   └── values-prod.yaml     # was charts/shelf/values-prod.yaml
└── observability/dashboards/
    └── shelf-overview.json  # was observability/dashboards/shelf-overview.json
```

## How the origin cluster consumes this

```
helm upgrade shelf charts/shelf \
  -f infra/penpencil/charts/shelf/values.yaml \
  -f infra/penpencil/charts/shelf/values-prod.yaml
```

The chart's default `values.yaml` is read first, then the two penpencil
overlays layer site-specific bucket / IRSA / hostname / replica-count
values on top.

## OSS publish hygiene

When tagging an OSS release (`v0.5`, `v1.0`, ...) the publish workflow is
expected to drop this directory before pushing the source tarball to
GitHub Releases / Helm OCI / crates:

```
git rm -r --cached infra/penpencil/
```

The directory is **not** in `.gitignore` today because we want it tracked
in-repo for the origin cluster's deploy workflow. The OSS workflow strips
it at publish time, which keeps history honest (yes, this project was
born on the penpencil cluster — see `docs/rollout-v1/` and `agents/out/`)
without shipping operational identifiers to downstream consumers.

## What does **not** belong here

- Anything that should stay in the OSS chart by default (probes,
  resource requests, NetworkPolicy structure, PDB) — keep in
  `charts/shelf/values.yaml`.
- Generic dashboards that any operator would want — when those land,
  put them at the canonical `observability/dashboards/` path with
  generic datasource UIDs.
- Secrets — never. Use IRSA + per-cluster k8s Secrets, not files in
  this repo. See `SECURITY.md`.
