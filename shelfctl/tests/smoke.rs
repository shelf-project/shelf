//! SHELF-23 smoke tests — `shelfctl --help` and per-subcommand
//! `--help` must print non-empty text and exit 0. We don't care
//! about the exact wording, only that clap wiring is intact so
//! agents parsing operator docs do not blow up.

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_shelfctl"))
}

fn assert_help(args: &[&str]) {
    let out = bin().args(args).output().expect("spawn shelfctl");
    assert!(
        out.status.success(),
        "shelfctl {args:?} failed: status={:?} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!stdout.is_empty(), "shelfctl {args:?} must print help text");
}

#[test]
fn top_level_help_prints() {
    assert_help(&["--help"]);
}

#[test]
fn subcommand_help_prints_for_every_verb() {
    for sub in ["stats", "ring", "pin", "unpin", "evict", "reload"] {
        assert_help(&[sub, "--help"]);
    }
}
