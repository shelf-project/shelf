//! SHELF-33 — `shelfctl install`.
//!
//! Auto-detect Trino catalogs in the current cluster, generate a
//! `values.yaml` for the Shelf Helm chart, ask once for confirmation,
//! and shell out to `helm upgrade --install`. The CLI side mirrors
//! what the user-facing `installer/install.sh` does so we can run it
//! either as a curl-pipe-sh installer or as a direct subcommand.
//!
//! The values.yaml synthesis is split out into [`generate_values`] so
//! the test suite can exercise it without a live apiserver.

use anyhow::{anyhow, Context, Result};
use clap::Args;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::api::ListParams;
use kube::config::Kubeconfig;
use kube::{Api, Client};
use std::collections::BTreeSet;
use std::io::Write;
use std::process::Stdio;
use tokio::process::Command as TokioCommand;

#[derive(Debug, Args)]
pub struct InstallArgs {
    /// Namespace to install Shelf into. (Distinct from the namespace
    /// where Trino runs.)
    #[arg(long, default_value = "shelf")]
    pub namespace: String,

    /// Helm release name for Shelf.
    #[arg(long, default_value = "shelf")]
    pub release: String,

    /// Helm chart reference. The default points at the published
    /// repo; override with a local path for dev loops.
    #[arg(long, default_value = "shelf-project/shelf")]
    pub chart: String,

    /// Label selector for Trino coordinator pods. Configurable so
    /// folks running a custom Helm chart can still self-detect.
    #[arg(
        long,
        default_value = "app.kubernetes.io/name=trino,app.kubernetes.io/component=coordinator"
    )]
    pub trino_selector: String,

    /// ServiceAccount name to bake into values.yaml.
    #[arg(long, default_value = "shelf")]
    pub service_account: String,

    /// Skip the interactive confirmation prompt. Implies "yes".
    #[arg(long)]
    pub yes: bool,

    /// Bypass the prod-cluster guard.
    #[arg(long)]
    pub force: bool,

    /// Print the generated values.yaml and exit; do not run helm.
    #[arg(long)]
    pub print_only: bool,
}

/// Inputs to [`generate_values`]. Populated either from live kube
/// reflection (real run) or from fixtures (unit tests).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InstallContext {
    pub service_account: String,
    pub replica_count: u32,
    /// Namespaces where Trino coordinators were found. Used to
    /// synthesise the NetworkPolicy ingress rules.
    pub trino_namespaces: BTreeSet<String>,
    /// Iceberg-relevant endpoints (S3, REST catalog, HMS) parsed from
    /// catalog ConfigMaps. Captured verbatim — operators can sanity
    /// check before applying.
    pub iceberg_endpoints: BTreeSet<String>,
}

pub async fn run(args: InstallArgs) -> Result<()> {
    let cluster_name = current_cluster_name().unwrap_or_default();
    if cluster_name.contains("prod") && !args.force {
        return Err(anyhow!(
            "refusing to install Shelf against cluster `{cluster_name}` (contains `prod`); pass --force to override"
        ));
    }

    let client = Client::try_default()
        .await
        .context("building kube client from kubeconfig / in-cluster config")?;

    let ctx = discover(&client, &args).await?;
    let yaml = generate_values(&ctx);
    let yaml_str = serde_yaml::to_string(&yaml).context("rendering values.yaml")?;

    println!("# generated values.yaml for shelf chart");
    println!("{yaml_str}");

    if args.print_only {
        return Ok(());
    }

    if !args.yes && !confirm("Apply this? [y/N] ")? {
        println!("aborted");
        return Ok(());
    }

    // Persist values.yaml next to cwd so helm can read it.
    let values_path = std::env::current_dir()?.join("values.yaml");
    std::fs::write(&values_path, &yaml_str)
        .with_context(|| format!("writing {}", values_path.display()))?;

    let status = TokioCommand::new("helm")
        .args([
            "upgrade",
            "--install",
            &args.release,
            &args.chart,
            "-n",
            &args.namespace,
            "-f",
        ])
        .arg(&values_path)
        .args(["--wait"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .context("spawning helm")?;
    if !status.success() {
        return Err(anyhow!("helm exited {status}"));
    }

    println!(
        "Shelf installed. Status: kubectl -n {} get pods -l app=shelf",
        args.namespace
    );
    Ok(())
}

async fn discover(client: &Client, args: &InstallArgs) -> Result<InstallContext> {
    // Coordinators across all namespaces — list with default and
    // accept that we may miss namespaces the kubeconfig user can't
    // see. We dedupe by (namespace, release-instance label) so we
    // count one replica set's worth, not raw pod count.
    let pods_api: Api<Pod> = Api::all(client.clone());
    let lp = ListParams::default().labels(&args.trino_selector);
    let pods = pods_api
        .list(&lp)
        .await
        .context("listing trino coordinator pods cluster-wide")?;

    let mut trino_namespaces = BTreeSet::new();
    let mut release_keys = BTreeSet::new();
    for pod in &pods.items {
        if let Some(ns) = pod.metadata.namespace.clone() {
            trino_namespaces.insert(ns.clone());
            let rel = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("app.kubernetes.io/instance").cloned())
                .unwrap_or_else(|| "trino".to_string());
            release_keys.insert((ns, rel));
        }
    }

    // For each (ns, release), pull the catalog ConfigMap and parse
    // iceberg endpoints. The conventional name is `<release>-trino-
    // catalog`; we fall back to scanning all CMs in the ns whose name
    // ends in `-catalog` if the conventional one is missing.
    let mut iceberg_endpoints = BTreeSet::new();
    for (ns, release) in &release_keys {
        let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);
        let primary = format!("{release}-trino-catalog");
        let mut cms: Vec<ConfigMap> = Vec::new();
        if let Ok(cm) = cm_api.get(&primary).await {
            cms.push(cm);
        } else if let Ok(list) = cm_api.list(&ListParams::default()).await {
            for cm in list.items {
                if cm
                    .metadata
                    .name
                    .as_deref()
                    .map(|n| n.ends_with("-catalog"))
                    .unwrap_or(false)
                {
                    cms.push(cm);
                }
            }
        }
        for cm in cms {
            if let Some(data) = cm.data {
                for (_filename, contents) in data {
                    for ep in extract_iceberg_endpoints(&contents) {
                        iceberg_endpoints.insert(ep);
                    }
                }
            }
        }
    }

    let replica_count = pods.items.len() as u32;
    Ok(InstallContext {
        service_account: args.service_account.clone(),
        replica_count: replica_count.max(1),
        trino_namespaces,
        iceberg_endpoints,
    })
}

/// Parse a Java-style `key=value` properties file body and pull out
/// the iceberg-relevant endpoint values. Unknown keys are ignored.
pub fn extract_iceberg_endpoints(props: &str) -> Vec<String> {
    let keys = [
        "s3.endpoint",
        "hive.metastore.uri",
        "iceberg.rest-catalog.uri",
        "iceberg.catalog.uri",
    ];
    let mut out = Vec::new();
    for line in props.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some(eq) = line.find('=') else { continue };
        let (k, v) = line.split_at(eq);
        let key = k.trim();
        let value = v[1..].trim().to_string();
        if keys.contains(&key) && !value.is_empty() {
            out.push(value);
        }
    }
    out
}

/// Render an `InstallContext` into the values.yaml structure the
/// Shelf chart expects. Pure function on purpose: every field is
/// inspectable and the unit tests can pin the schema.
pub fn generate_values(ctx: &InstallContext) -> serde_yaml::Value {
    use serde_yaml::{Mapping, Value};

    let mut root = Mapping::new();

    // serviceAccount.name
    let mut sa = Mapping::new();
    sa.insert(
        Value::String("name".into()),
        Value::String(ctx.service_account.clone()),
    );
    root.insert(Value::String("serviceAccount".into()), Value::Mapping(sa));

    // replicaCount
    root.insert(
        Value::String("replicaCount".into()),
        Value::Number(ctx.replica_count.into()),
    );

    // networkPolicy.ingress.from = [{ namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: <ns> } } }, ...]
    let from_list: Vec<Value> = ctx
        .trino_namespaces
        .iter()
        .map(|ns| {
            let mut entry = Mapping::new();
            let mut nss = Mapping::new();
            let mut match_labels = Mapping::new();
            match_labels.insert(
                Value::String("kubernetes.io/metadata.name".into()),
                Value::String(ns.clone()),
            );
            nss.insert(
                Value::String("matchLabels".into()),
                Value::Mapping(match_labels),
            );
            entry.insert(
                Value::String("namespaceSelector".into()),
                Value::Mapping(nss),
            );
            Value::Mapping(entry)
        })
        .collect();
    let mut ingress = Mapping::new();
    ingress.insert(Value::String("from".into()), Value::Sequence(from_list));
    let mut np = Mapping::new();
    np.insert(Value::String("ingress".into()), Value::Mapping(ingress));
    root.insert(Value::String("networkPolicy".into()), Value::Mapping(np));

    // trino.icebergEndpoints — informational, lets operators eyeball
    // what we found before approving.
    if !ctx.iceberg_endpoints.is_empty() {
        let mut trino_block = Mapping::new();
        let endpoints: Vec<Value> = ctx
            .iceberg_endpoints
            .iter()
            .cloned()
            .map(Value::String)
            .collect();
        trino_block.insert(
            Value::String("icebergEndpoints".into()),
            Value::Sequence(endpoints),
        );
        root.insert(Value::String("trino".into()), Value::Mapping(trino_block));
    }

    Value::Mapping(root)
}

fn current_cluster_name() -> Option<String> {
    let kc = Kubeconfig::read().ok()?;
    let current = kc.current_context.clone()?;
    kc.contexts
        .iter()
        .find(|c| c.name == current)
        .and_then(|c| c.context.as_ref())
        .map(|c| c.cluster.clone())
}

fn confirm(prompt: &str) -> Result<bool> {
    let mut stdout = std::io::stdout();
    write!(stdout, "{prompt}")?;
    stdout.flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let answer = line.trim().to_ascii_lowercase();
    Ok(matches!(answer.as_str(), "y" | "yes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with(ns: &[&str], reps: u32) -> InstallContext {
        InstallContext {
            service_account: "shelf".to_string(),
            replica_count: reps,
            trino_namespaces: ns.iter().map(|s| s.to_string()).collect(),
            iceberg_endpoints: BTreeSet::new(),
        }
    }

    #[test]
    fn values_yaml_carries_service_account_name() {
        let v = generate_values(&ctx_with(&["trino"], 2));
        let m = v.as_mapping().expect("root is mapping");
        let sa = m
            .get(serde_yaml::Value::String("serviceAccount".into()))
            .expect("serviceAccount present")
            .as_mapping()
            .unwrap();
        assert_eq!(
            sa.get(serde_yaml::Value::String("name".into()))
                .and_then(|v| v.as_str()),
            Some("shelf")
        );
    }

    #[test]
    fn values_yaml_replica_count_matches_input() {
        let v = generate_values(&ctx_with(&["trino"], 3));
        let m = v.as_mapping().unwrap();
        assert_eq!(
            m.get(serde_yaml::Value::String("replicaCount".into()))
                .and_then(|v| v.as_u64()),
            Some(3)
        );
    }

    #[test]
    fn values_yaml_network_policy_has_one_entry_per_namespace() {
        let v = generate_values(&ctx_with(&["trino-a", "trino-b", "data-eng"], 1));
        let m = v.as_mapping().unwrap();
        let np = m
            .get(serde_yaml::Value::String("networkPolicy".into()))
            .unwrap()
            .as_mapping()
            .unwrap();
        let ingress = np
            .get(serde_yaml::Value::String("ingress".into()))
            .unwrap()
            .as_mapping()
            .unwrap();
        let from = ingress
            .get(serde_yaml::Value::String("from".into()))
            .unwrap()
            .as_sequence()
            .unwrap();
        assert_eq!(from.len(), 3);
        let names: Vec<&str> = from
            .iter()
            .map(|e| {
                e.as_mapping()
                    .unwrap()
                    .get(serde_yaml::Value::String("namespaceSelector".into()))
                    .unwrap()
                    .as_mapping()
                    .unwrap()
                    .get(serde_yaml::Value::String("matchLabels".into()))
                    .unwrap()
                    .as_mapping()
                    .unwrap()
                    .get(serde_yaml::Value::String(
                        "kubernetes.io/metadata.name".into(),
                    ))
                    .unwrap()
                    .as_str()
                    .unwrap()
            })
            .collect();
        // BTreeSet guarantees sorted order — pin that here.
        assert_eq!(names, vec!["data-eng", "trino-a", "trino-b"]);
    }

    #[test]
    fn extract_iceberg_endpoints_picks_known_keys_only() {
        let body = r#"
# iceberg catalog
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://rest-catalog.svc:8181
s3.endpoint=https://s3.example.local
hive.metastore.uri=thrift://hms.svc:9083
ignored.key=foo
"#;
        let mut got = extract_iceberg_endpoints(body);
        got.sort();
        assert_eq!(
            got,
            vec![
                "http://rest-catalog.svc:8181".to_string(),
                "https://s3.example.local".to_string(),
                "thrift://hms.svc:9083".to_string(),
            ]
        );
    }

    #[test]
    fn values_yaml_emits_iceberg_endpoints_block_when_present() {
        let mut ctx = ctx_with(&["trino"], 1);
        ctx.iceberg_endpoints.insert("http://rest:8181".into());
        let v = generate_values(&ctx);
        let m = v.as_mapping().unwrap();
        let trino = m
            .get(serde_yaml::Value::String("trino".into()))
            .unwrap()
            .as_mapping()
            .unwrap();
        let eps = trino
            .get(serde_yaml::Value::String("icebergEndpoints".into()))
            .unwrap()
            .as_sequence()
            .unwrap();
        assert_eq!(eps.len(), 1);
        assert_eq!(eps[0].as_str(), Some("http://rest:8181"));
    }

    #[test]
    fn values_yaml_omits_trino_block_when_no_endpoints() {
        let v = generate_values(&ctx_with(&["trino"], 1));
        let m = v.as_mapping().unwrap();
        assert!(m.get(serde_yaml::Value::String("trino".into())).is_none());
    }
}
