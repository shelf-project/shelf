//! SHELF-31 — `shelfctl chaos`.
//!
//! Kill a random fraction of `shelfd` pods in a namespace so the
//! load-test harness can prove that the read path is fail-open. The
//! command is deliberately small: list pods → pick victims → DELETE
//! with grace-period 0 → print before/after counts.
//!
//! Safety rails:
//! - The cluster name is read from kubeconfig and the command refuses
//!   to run if it contains `prod` unless `--force` is passed.
//! - `--dry-run` lists the targets without touching the apiserver.

use anyhow::{anyhow, Context, Result};
use clap::Args;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams};
use kube::config::Kubeconfig;
use kube::{Api, Client};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Args)]
pub struct ChaosArgs {
    /// Namespace whose shelfd pods to perturb.
    #[arg(long, default_value = "alluxio")]
    pub namespace: String,

    /// Label selector identifying shelfd pods.
    #[arg(long, default_value = "app=shelf")]
    pub selector: String,

    /// Fraction of matching pods to kill. Accepts `50%` or a decimal
    /// like `0.5`.
    #[arg(long, default_value = "50%")]
    pub kill: String,

    /// List target pods without deleting.
    #[arg(long)]
    pub dry_run: bool,

    /// Bypass the prod-cluster guard.
    #[arg(long)]
    pub force: bool,
}

pub async fn run(args: ChaosArgs) -> Result<()> {
    let cluster_name = current_cluster_name().unwrap_or_default();
    if cluster_name.contains("prod") && !args.force {
        return Err(anyhow!(
            "refusing to run chaos against cluster `{cluster_name}` (contains `prod`); pass --force to override"
        ));
    }

    let fraction =
        parse_fraction(&args.kill).with_context(|| format!("parsing --kill {:?}", args.kill))?;

    let client = Client::try_default()
        .await
        .context("building kube client from kubeconfig / in-cluster config")?;
    let api: Api<Pod> = Api::namespaced(client, &args.namespace);

    let lp = ListParams::default().labels(&args.selector);
    let pods = api.list(&lp).await.with_context(|| {
        format!(
            "listing pods in ns={} selector={}",
            args.namespace, args.selector
        )
    })?;

    let names: Vec<String> = pods
        .items
        .iter()
        .filter_map(|p| p.metadata.name.clone())
        .collect();
    let total = names.len();
    println!(
        "matched {total} pods in ns={} selector={}",
        args.namespace, args.selector
    );

    if total == 0 {
        return Ok(());
    }

    let target_count = ((total as f64) * fraction).ceil() as usize;
    let target_count = target_count.min(total);
    let victims = pick_random(&names, target_count);

    println!(
        "targeting {} pods (fraction={:.3}):",
        victims.len(),
        fraction
    );
    for name in &victims {
        println!("  - {name}");
    }

    if args.dry_run {
        println!("dry-run: no pods deleted");
        return Ok(());
    }

    let dp = DeleteParams::default().grace_period(0);
    let mut killed = 0usize;
    for name in &victims {
        match api.delete(name, &dp).await {
            Ok(_) => {
                killed += 1;
                println!("  deleted {name}");
            }
            Err(e) => eprintln!("  failed to delete {name}: {e}"),
        }
    }

    // Re-list so the after-count reflects what the apiserver thinks
    // (terminating pods may still be visible until grace-period 0
    // takes effect, but that's fine — operators want the truth).
    let after = api.list(&lp).await.map(|l| l.items.len()).unwrap_or(total);
    println!("before={total} after={after} killed={killed}");
    Ok(())
}

/// Read the current kubeconfig and return the cluster name bound to
/// the active context. Returns `None` if no kubeconfig is available
/// (e.g. running in a sidecar with only in-cluster config).
fn current_cluster_name() -> Option<String> {
    let kc = Kubeconfig::read().ok()?;
    let current = kc.current_context.clone()?;
    kc.contexts
        .iter()
        .find(|c| c.name == current)
        .and_then(|c| c.context.as_ref())
        .map(|c| c.cluster.clone())
}

/// Parse `"50%"` → `0.5`, `"0.25"` → `0.25`. Anything outside `(0,1]`
/// is rejected so an operator can't accidentally kill 0 or >100% of a
/// fleet.
pub(crate) fn parse_fraction(s: &str) -> Result<f64> {
    let trimmed = s.trim();
    let v: f64 = if let Some(stripped) = trimmed.strip_suffix('%') {
        stripped
            .trim()
            .parse::<f64>()
            .map_err(|_| anyhow!("invalid percent: {s}"))?
            / 100.0
    } else {
        trimmed
            .parse::<f64>()
            .map_err(|_| anyhow!("invalid fraction: {s}"))?
    };
    if !(v > 0.0 && v <= 1.0) {
        return Err(anyhow!("fraction {v} not in (0, 1]"));
    }
    Ok(v)
}

/// Tiny xorshift64 shuffle so we don't need to pull in `rand`. The
/// caller is the chaos harness — cryptographic randomness would be
/// overkill, and a time-seeded PRNG keeps the dep graph trim.
fn pick_random(names: &[String], k: usize) -> Vec<String> {
    let n = names.len();
    if k >= n {
        return names.to_vec();
    }
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0xDEADBEEF);
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).max(1);

    let mut idx: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        idx.swap(i, j);
    }
    idx.into_iter().take(k).map(|i| names[i].clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_percent_form() {
        assert!((parse_fraction("50%").unwrap() - 0.5).abs() < 1e-9);
        assert!((parse_fraction("100%").unwrap() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn parse_decimal_form() {
        assert!((parse_fraction("0.25").unwrap() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn reject_out_of_range() {
        assert!(parse_fraction("0%").is_err());
        assert!(parse_fraction("150%").is_err());
        assert!(parse_fraction("-1").is_err());
        assert!(parse_fraction("garbage").is_err());
    }

    #[test]
    fn pick_random_returns_requested_count() {
        let names: Vec<String> = (0..10).map(|i| format!("p{i}")).collect();
        let picked = pick_random(&names, 4);
        assert_eq!(picked.len(), 4);
        for p in &picked {
            assert!(names.contains(p));
        }
    }
}
