use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use tracing::trace;

use crate::{
    TreeEnumerator, TreeReader, TreeWriter,
    fsutil::{self, DATA, TREE},
    types::ContentHash,
};

pub struct FsStore {
    root: PathBuf,
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

impl FsStore {
    pub fn new(root: impl AsRef<Path>) -> IoResult<Self> {
        let root = root.as_ref().to_path_buf();
        fsutil::create_layout(&root)?;
        Ok(Self { root })
    }

    fn object_path(&self, hash: &ContentHash, is_data: bool) -> PathBuf {
        fsutil::object_path(&self.root, hash, is_data)
    }
}

impl TreeReader for FsStore {
    type Error = std::io::Error;

    async fn read(&self, hash: &ContentHash, is_data: bool) -> IoResult<Option<Vec<u8>>> {
        let path = self.object_path(&hash, is_data);
        trace!("Reading object {}", path.display());
        let data = match std::fs::read(&path) {
            Ok(data) => Some(data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };

        Ok(data)
    }
}

impl TreeWriter for FsStore {
    async fn write(&self, hash: &ContentHash, data: Vec<u8>, is_data: bool) -> IoResult<()> {
        let path = self.object_path(hash, is_data);
        if !path.exists() {
            fsutil::write_atomic(&path, &data)?;
        }
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash, is_data: bool) -> IoResult<()> {
        let path = self.object_path(&hash, is_data);
        std::fs::remove_file(&path)?;
        Ok(())
    }
}

impl TreeEnumerator for FsStore {
    async fn enumerate_all(&self) -> IoResult<HashSet<(ContentHash, bool)>> {
        let mut all = HashSet::new();
        let objects_dir = self.root.join(TREE);
        for entry in std::fs::read_dir(objects_dir)? {
            // Skips anything that isn't a hash, like leftover `.tmp` files.
            if let Some(hash) = parse_hash(&entry?.file_name().to_string_lossy()) {
                all.insert((hash, false));
            }
        }
        let data_dir = self.root.join(DATA);
        for entry in std::fs::read_dir(&data_dir)? {
            let pfx = entry?;
            if pfx.metadata()?.is_dir() {
                for entry in std::fs::read_dir(pfx.path())? {
                    let hash = format!(
                        "{}{}",
                        pfx.file_name().to_string_lossy(),
                        entry?.file_name().to_string_lossy()
                    );
                    if let Some(hash) = parse_hash(&hash) {
                        all.insert((hash, true));
                    }
                }
            }
        }
        Ok(all)
    }
}

fn parse_hash(name: &str) -> Option<ContentHash> {
    ContentHash::try_from(name).ok()
}
