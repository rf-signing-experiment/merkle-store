use std::{
    collections::HashSet,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use tracing::trace;

use crate::{TreeEnumerator, TreeReader, TreeWriter, types::ContentHash};

const TREE: &str = "tree";
const DATA: &str = "data";

/// Stores objects as files named by their SHA-256 in hex: tree nodes under
/// `tree/<sha256>`, data blobs under `data/<first 2 hex>/<rest>`.
pub struct FsStore {
    root: PathBuf,
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

impl FsStore {
    pub fn new(root: impl AsRef<Path>) -> IoResult<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(root.join(TREE))?;
        std::fs::create_dir_all(root.join(DATA))?;
        Ok(Self { root })
    }

    fn object_path(&self, hash: &ContentHash, is_data: bool) -> PathBuf {
        let hex = hash.to_string();
        if is_data {
            self.root.join(DATA).join(&hex[0..2]).join(&hex[2..])
        } else {
            self.root.join(TREE).join(hex)
        }
    }
}

/// Writes `data` to `path` via `<path>.tmp` and a rename, so `path` never
/// holds a partial file.
fn write_atomic(path: &Path, data: &[u8]) -> IoResult<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)
}

impl TreeReader for FsStore {
    type Error = std::io::Error;

    async fn read(
        &self,
        hash: &ContentHash,
        _size_limit: usize,
        is_data: bool,
    ) -> IoResult<Option<Vec<u8>>> {
        let path = self.object_path(hash, is_data);
        trace!("Reading object {}", path.display());
        match std::fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl TreeWriter for FsStore {
    async fn write(&self, hash: &ContentHash, data: Vec<u8>, is_data: bool) -> IoResult<()> {
        let path = self.object_path(hash, is_data);
        if !path.exists() {
            write_atomic(&path, &data)?;
        }
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash, is_data: bool) -> IoResult<()> {
        let path = self.object_path(hash, is_data);
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }
}

impl TreeEnumerator for FsStore {
    /// Skips names that aren't a SHA-256 in hex, like leftover `.tmp` files.
    async fn enumerate_all(&self) -> IoResult<HashSet<(ContentHash, bool)>> {
        let mut all = HashSet::new();
        let objects_dir = self.root.join(TREE);
        for entry in std::fs::read_dir(objects_dir)? {
            if let Ok(hash) = entry?.file_name().to_string_lossy().parse() {
                all.insert((hash, false));
            }
        }
        let data_dir = self.root.join(DATA);
        for entry in std::fs::read_dir(&data_dir)? {
            let pfx = entry?;
            if pfx.metadata()?.is_dir() {
                for entry in std::fs::read_dir(pfx.path())? {
                    let hex = format!(
                        "{}{}",
                        pfx.file_name().to_string_lossy(),
                        entry?.file_name().to_string_lossy()
                    );
                    if let Ok(hash) = hex.parse() {
                        all.insert((hash, true));
                    }
                }
            }
        }
        Ok(all)
    }
}
