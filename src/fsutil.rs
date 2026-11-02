//! Helpers shared by the filesystem backends.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub(crate) const TREE: &str = "tree";
pub(crate) const DATA: &str = "data";

/// Creates the `tree` and `data` directories under `root`.
pub(crate) fn create_layout(root: &Path) -> io::Result<()> {
    for dir in [TREE, DATA] {
        fs::create_dir_all(root.join(dir))?;
    }
    Ok(())
}

/// Path of an object: `tree/<hex>` for nodes, `data/<2 hex>/<rest>` for blobs.
pub(crate) fn object_path(root: &Path, hash: &crate::ContentHash, is_data: bool) -> PathBuf {
    let hash = hash.to_string();
    if is_data {
        root.join(DATA).join(&hash[0..2]).join(&hash[2..])
    } else {
        root.join(TREE).join(hash)
    }
}

/// Writes `data` to `path` via `<path>.tmp` and a rename, so `path` never
/// holds a partial file.
pub(crate) fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    fs::create_dir_all(path.parent().unwrap())?;
    fs::write(&tmp, data)?;
    fs::rename(&tmp, path)
}
