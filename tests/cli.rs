//! Reach each command through the real binary, and compare the commands that the readme lists with the commands of
//! the binary.

use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pairpack"))
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn run(args: &[&str]) -> (bool, String) {
    let out = bin().args(args).output().unwrap();
    (out.status.success(), String::from_utf8(out.stdout).unwrap())
}

#[test]
fn packs_verifies_unpacks_and_reports_through_the_binary() {
    let dir = tempfile::tempdir().unwrap();
    let packed = dir.path().join("packed");
    let unpacked = dir.path().join("unpacked");
    let src = fixture("corrections.parquet");
    let (src, packed_s, unpacked_s) = (src.to_str().unwrap(), packed.to_str().unwrap(), unpacked.to_str().unwrap());
    let shard = packed.join("corrections.parquet");
    let shard = shard.to_str().unwrap();
    let fixtures = fixture("");
    let fixtures = fixtures.to_str().unwrap();

    let (ok, out) = run(&["--workers", "2", "pack", "--keep", "response", "--derive", "text", "--out", packed_s, src]);
    assert!(ok && out.contains("\"rows verified\":8"), "{out}");
    let (ok, out) = run(&["verify", "--original", fixtures, shard]);
    assert!(ok && out.contains("\"rows verified\":8"), "{out}");
    let (ok, out) = run(&["unpack", "--out", unpacked_s, shard]);
    assert!(ok && out.contains("\"rows verified\":8"), "{out}");
    let (ok, out) = run(&["stats", shard]);
    assert!(ok && out.contains("\"packed\":true"), "{out}");
}

#[test]
fn fails_and_names_the_shard_that_failed() {
    let dir = tempfile::tempdir().unwrap();
    let src = fixture("spoken.parquet");
    let out = bin()
        .args([
            "pack",
            "--keep",
            "text",
            "--derive",
            "missing",
            "--out",
            dir.path().to_str().unwrap(),
            src.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8(out.stdout).unwrap().contains("\"error\""));
}

/// The command names from the help of the binary.
fn commands() -> Vec<String> {
    let (_, help) = run(&["--help"]);
    let section = help.split("Commands:").nth(1).unwrap().split("Options:").next().unwrap();
    section.lines().filter_map(|l| l.split_whitespace().next()).filter(|c| *c != "help").map(String::from).collect()
}

#[test]
fn lists_every_command_in_the_readme() {
    let readme = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md")).unwrap();
    let listed: Vec<String> = readme
        .lines()
        .filter_map(|l| l.trim_start().strip_prefix("pairpack "))
        .filter_map(|rest| {
            rest.split_whitespace().find(|w| !w.starts_with('-') && w.chars().all(|c| c.is_ascii_lowercase()))
        })
        .map(String::from)
        .collect();
    let mut want = commands();
    want.sort();
    let mut got = listed;
    got.sort();
    got.dedup();
    assert_eq!(got, want);
}
