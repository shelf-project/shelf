//! SHELF-32 — `shelfctl bundle`.
//!
//! Produces a single `.tar.gz` of diagnostic state for a `shelfd`
//! cluster — modelled after OpenShift `must-gather`. For each pod we
//! capture `kubectl logs --tail`, plus `/stats`, `/metrics`,
//! `/admin/ring` over a short-lived `kubectl port-forward`. We also
//! grab `helm get values` if a release name is supplied.
//!
//! Secrets get stripped via a regex pass over every text file before
//! the tar gets sealed. The redactors live in [`redact`] so we can
//! unit-test them without spinning up a fake apiserver.

use anyhow::{anyhow, Context, Result};
use clap::Args;
use flate2::write::GzEncoder;
use flate2::Compression;
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client};
use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command as TokioCommand;
use tokio::time::sleep;

#[derive(Debug, Args)]
pub struct BundleArgs {
    /// Namespace to gather from.
    #[arg(long, default_value = "alluxio")]
    pub namespace: String,

    /// Label selector identifying shelfd pods.
    #[arg(long, default_value = "app=shelf")]
    pub selector: String,

    /// Helm release name. If supplied, `helm get values <release>` is
    /// captured. Skipped (with warning) if `helm` is not on PATH.
    #[arg(long)]
    pub helm_release: Option<String>,

    /// Disable redaction. The bundle will contain raw secrets — only
    /// use when you control the destination.
    #[arg(long)]
    pub full: bool,

    /// Local TCP port base for the kubectl port-forward span. Each
    /// pod uses base+i to avoid collisions.
    #[arg(long, default_value_t = 18080)]
    pub local_port_base: u16,
}

pub async fn run(args: BundleArgs) -> Result<()> {
    let ts = epoch_seconds();
    let workdir_name = format!("shelfctl-bundle-{ts}");
    let workdir = std::env::temp_dir().join(&workdir_name);
    fs::create_dir_all(&workdir)
        .with_context(|| format!("creating workdir {}", workdir.display()))?;

    let client = Client::try_default()
        .await
        .context("building kube client")?;
    let api: Api<Pod> = Api::namespaced(client, &args.namespace);
    let lp = ListParams::default().labels(&args.selector);
    let pods = api
        .list(&lp)
        .await
        .with_context(|| format!("listing pods ns={} selector={}", args.namespace, args.selector))?;

    let pod_names: Vec<String> = pods
        .items
        .iter()
        .filter_map(|p| p.metadata.name.clone())
        .collect();
    println!("collecting from {} pods", pod_names.len());

    for (i, pod) in pod_names.iter().enumerate() {
        let pod_dir = workdir.join(format!("pod-{pod}"));
        fs::create_dir_all(&pod_dir)?;
        if let Err(e) = collect_logs(&args.namespace, pod, &pod_dir).await {
            eprintln!("  {pod}: logs: {e}");
        }
        let local_port = args.local_port_base + i as u16;
        if let Err(e) = collect_http(&args.namespace, pod, local_port, &pod_dir).await {
            eprintln!("  {pod}: http: {e}");
        }
    }

    if let Some(release) = &args.helm_release {
        let helm_path = workdir.join("helm-values.yaml");
        if let Err(e) = collect_helm_values(release, &args.namespace, &helm_path).await {
            eprintln!("  helm: {e}");
        }
    }

    if !args.full {
        redact_tree(&workdir).context("redaction pass")?;
    } else {
        eprintln!("WARNING: --full disables redaction; bundle will contain unredacted secrets");
    }

    let out = std::env::current_dir()?.join(format!("shelfctl-bundle-{ts}.tar.gz"));
    write_tarball(&workdir, &workdir_name, &out)?;
    println!("wrote {}", out.display());
    Ok(())
}

fn epoch_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn collect_logs(ns: &str, pod: &str, dest_dir: &Path) -> Result<()> {
    let out = TokioCommand::new("kubectl")
        .args(["logs", "-n", ns, pod, "--tail=10000"])
        .output()
        .await
        .context("spawn kubectl logs")?;
    let dest = dest_dir.join("logs.txt");
    fs::write(&dest, out.stdout)?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(anyhow!("kubectl logs exit {}: {stderr}", out.status));
    }
    Ok(())
}

async fn collect_http(ns: &str, pod: &str, local_port: u16, dest_dir: &Path) -> Result<()> {
    // Spawn `kubectl port-forward` in the background. We detach
    // stdout/stderr to /dev/null-ish (capture so the OS pipe buffer
    // doesn't fill) and rely on a 1.5s warmup before issuing the
    // first request. The child is killed on drop.
    let mut child = TokioCommand::new("kubectl")
        .args([
            "port-forward",
            "-n",
            ns,
            &format!("pod/{pod}"),
            &format!("{local_port}:8080"),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawn kubectl port-forward")?;

    sleep(Duration::from_millis(1500)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let base = format!("http://127.0.0.1:{local_port}");
    let endpoints = [
        ("stats.json", "/stats"),
        ("metrics.txt", "/metrics"),
        ("admin-ring.json", "/admin/ring"),
    ];
    let mut last_err: Option<anyhow::Error> = None;
    for (filename, path) in endpoints {
        match client.get(format!("{base}{path}")).send().await {
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                fs::write(dest_dir.join(filename), body)?;
            }
            Err(e) => {
                last_err = Some(anyhow!("GET {path}: {e}"));
            }
        }
    }

    // Drain any pending stderr from kubectl port-forward so it
    // shows up in operator logs if something went wrong.
    if let Some(mut stderr) = child.stderr.take() {
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_millis(50), stderr.read_to_end(&mut buf)).await;
        if !buf.is_empty() {
            eprintln!(
                "  {pod}: port-forward stderr: {}",
                String::from_utf8_lossy(&buf)
            );
        }
    }
    let _ = child.start_kill();
    let _ = child.wait().await;

    match last_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

async fn collect_helm_values(release: &str, ns: &str, dest: &Path) -> Result<()> {
    if which("helm").is_none() {
        eprintln!("WARNING: `helm` not on PATH; skipping helm get values");
        return Ok(());
    }
    let out = TokioCommand::new("helm")
        .args(["get", "values", release, "-n", ns])
        .output()
        .await
        .context("spawn helm get values")?;
    fs::write(dest, out.stdout)?;
    if !out.status.success() {
        return Err(anyhow!(
            "helm get values exit {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(())
}

fn which(cmd: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(cmd);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn redact_tree(root: &Path) -> Result<()> {
    let redactors = redact::redactors();
    visit(root, &|path: &Path| -> Result<()> {
        if !is_text_file(path) {
            return Ok(());
        }
        let content = match fs::read_to_string(path) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        let redacted = redact::apply(&content, &redactors);
        if redacted != content {
            fs::write(path, redacted)?;
        }
        Ok(())
    })
}

fn visit(dir: &Path, f: &dyn Fn(&Path) -> Result<()>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            visit(&path, f)?;
        } else {
            f(&path)?;
        }
    }
    Ok(())
}

fn is_text_file(path: &Path) -> bool {
    match path.extension().and_then(|s| s.to_str()) {
        Some("txt" | "log" | "json" | "yaml" | "yml" | "properties" | "conf" | "ini") => true,
        _ => true, // we generated everything; default to redact
    }
}

fn write_tarball(workdir: &Path, root_name: &str, out: &Path) -> Result<()> {
    let file = fs::File::create(out)
        .with_context(|| format!("creating {}", out.display()))?;
    let enc = GzEncoder::new(file, Compression::default());
    let mut tar = tar::Builder::new(enc);
    tar.append_dir_all(root_name, workdir)?;
    tar.finish()?;
    Ok(())
}

/// Redaction primitives live in their own submodule so `cargo test`
/// can exercise them in isolation. The intent is "blunt instrument":
/// false positives are fine, leakage is not.
pub mod redact {
    use regex::Regex;

    pub struct Redactor {
        pub re: Regex,
        pub replacement: &'static str,
    }

    pub fn redactors() -> Vec<Redactor> {
        vec![
            Redactor {
                re: Regex::new(r"arn:aws:iam::\d+:role/[\w\-/+=,.@]+")
                    .expect("IRSA ARN regex compiles"),
                replacement: "arn:aws:iam::REDACTED:role/REDACTED",
            },
            Redactor {
                re: Regex::new(r"AKIA[0-9A-Z]{16}").expect("AWS access key regex compiles"),
                replacement: "AKIA<REDACTED>",
            },
            Redactor {
                re: Regex::new(r"Bearer [A-Za-z0-9._\-]+").expect("bearer token regex compiles"),
                replacement: "Bearer <REDACTED>",
            },
            Redactor {
                re: Regex::new(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
                    .expect("IPv4 regex compiles"),
                replacement: "<IP>",
            },
        ]
    }

    pub fn apply(input: &str, redactors: &[Redactor]) -> String {
        let mut out = input.to_string();
        for r in redactors {
            out = r.re.replace_all(&out, r.replacement).into_owned();
        }
        out
    }
}

// Silence unused-import warnings if features ever pare down.
#[allow(dead_code)]
fn _unused_re_anchor(_r: &Regex) {}

#[cfg(test)]
mod tests {
    use super::redact::{apply, redactors};

    #[test]
    fn redacts_irsa_arn() {
        let s = "role: arn:aws:iam::123456789012:role/shelf-irsa-role-abc.def";
        let out = apply(s, &redactors());
        assert!(
            out.contains("arn:aws:iam::REDACTED:role/REDACTED"),
            "unexpected: {out}"
        );
        assert!(!out.contains("123456789012"));
    }

    #[test]
    fn redacts_aws_access_key_and_bearer() {
        let s = "key=AKIAABCDEFGHIJKLMNOP token=Bearer abc.def-ghi_jkl";
        let out = apply(s, &redactors());
        assert!(out.contains("AKIA<REDACTED>"), "unexpected: {out}");
        assert!(out.contains("Bearer <REDACTED>"), "unexpected: {out}");
        assert!(!out.contains("AKIAABCDEFGHIJKLMNOP"));
        assert!(!out.contains("abc.def-ghi_jkl"));
    }

    #[test]
    fn redacts_ipv4() {
        let s = "peer 10.0.1.42 connected to 192.168.5.6:8080";
        let out = apply(s, &redactors());
        assert!(out.contains("<IP>"), "unexpected: {out}");
        assert!(!out.contains("10.0.1.42"));
        assert!(!out.contains("192.168.5.6"));
    }

    #[test]
    fn does_not_touch_clean_text() {
        let s = "shelfd booted; pool=rowgroup; replicas=3";
        let out = apply(s, &redactors());
        assert_eq!(out, s);
    }
}
