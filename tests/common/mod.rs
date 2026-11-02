//! Helpers shared by the filesystem backend tests.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use merkletree::ContentHash;

/// A fresh, empty directory under `target/test-tmp/`.
pub fn temp_dir(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join(format!("{name}_{}_{count}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Where the filesystem backends store an object, by its SHA-256 in hex:
/// `data/<2 hex>/<rest>` for data blobs, `tree/<hex>` for tree nodes.
pub fn object_path(root: &Path, hash: &ContentHash, is_data: bool) -> PathBuf {
    let hex = hex::encode(hash.hash());
    if is_data {
        root.join("data").join(&hex[0..2]).join(&hex[2..])
    } else {
        root.join("tree").join(hex)
    }
}
