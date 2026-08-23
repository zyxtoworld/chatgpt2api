use std::fs;
use std::path::{Path, PathBuf};

fn collect_rust_sources(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("read Rust source directory") {
        let entry = entry.expect("read Rust source entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn rust_sources_do_not_use_process_temp_directory_directly() {
    let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_sources(&source_root, &mut files);

    let forbidden = ["std::env::temp_dir()", "env::temp_dir()"];
    let mut offenders = Vec::new();
    for path in files {
        let source = fs::read_to_string(&path).expect("read Rust source");
        if forbidden.iter().any(|needle| source.contains(needle)) {
            offenders.push(path.display().to_string());
        }
    }

    assert!(
        offenders.is_empty(),
        "Rust source must use the project-local test temp helper: {}",
        offenders.join(", ")
    );
}
