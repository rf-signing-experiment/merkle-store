use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use tracing::trace;

use crate::{TreeEnumerator, TreeReader, TreeWriter, types::ContentHash};

pub struct FsStore {
    root: PathBuf,
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

const TREE: &'static str = "tree";
const DATA: &'static str = "data";

impl FsStore {
    pub fn new(root: impl AsRef<Path>) -> IoResult<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(root.join(TREE))?;
        std::fs::create_dir_all(root.join(DATA))?;
        Ok(Self { root })
    }

    fn object_path(&self, hash: &ContentHash, is_leaf: bool) -> PathBuf {
        let hash = hash.to_string();
        if is_leaf {
            self.root.join(DATA).join(&hash[0..2]).join(&hash[2..])
        } else {
            self.root.join(TREE).join(hash)
        }
    }
}

impl TreeReader for FsStore {
    type Error = std::io::Error;

    async fn read(&self, hash: &ContentHash, is_leaf: bool) -> IoResult<Option<Vec<u8>>> {
        let path = self.object_path(&hash, is_leaf);
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
    async fn write(&self, hash: &ContentHash, data: Vec<u8>, is_leaf: bool) -> IoResult<()> {
        let path = self.object_path(&hash, is_leaf);
        std::fs::create_dir_all(path.parent().unwrap())?;
        if !path.exists() {
            std::fs::write(&path, data)?;
        }
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash, is_leaf: bool) -> IoResult<()> {
        let path = self.object_path(&hash, is_leaf);
        std::fs::remove_file(&path)?;
        Ok(())
    }
}

impl TreeEnumerator for FsStore {
    async fn enumerate_all(&self) -> IoResult<HashSet<(ContentHash, bool)>> {
        let mut all = HashSet::new();
        let objects_dir = self.root.join(TREE);
        for entry in std::fs::read_dir(objects_dir)? {
            let entry = entry?;
            let hash = entry.file_name().to_str().unwrap().to_string();
            all.insert((hash.as_str().try_into().unwrap(), false));
        }
        let data_dir = self.root.join(DATA);
        for entry in std::fs::read_dir(&data_dir)? {
            let pfx = entry?;
            if pfx.metadata()?.is_dir() {
                for entry in std::fs::read_dir(pfx.path())? {
                    let hash = format!(
                        "{}{}",
                        pfx.file_name().to_str().unwrap(),
                        entry?.file_name().to_str().unwrap()
                    );
                    all.insert((hash.as_str().try_into().unwrap(), true));
                }
            }
        }
        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute_hash;
    use futures::executor::block_on;
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let count = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(format!("fsstore_test_{}_{}", std::process::id(), count));
        let _ = fs::remove_dir_all(&dir); // Clean up any previous test
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_fsstore_new() {
        let temp = temp_dir();
        let store = FsStore::new(&temp);
        assert!(store.is_ok());
        let store = store.unwrap();
        assert!(store.root.join(TREE).exists());
        assert!(store.root.join(DATA).exists());
    }

    #[test]
    fn test_write_read_leaf() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();
        let data = b"hello world";
        let hash = compute_hash(data);

        block_on(async {
            store.write(&hash, data.to_vec(), true).await.unwrap();
            let read_data = store.read(&hash, true).await.unwrap().unwrap();
            assert_eq!(read_data, data);
            // The same hash in the other namespace is a different object.
            assert!(store.read(&hash, false).await.unwrap().is_none());
        });
    }

    #[test]
    fn test_write_read_non_leaf() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();
        let data = b"{\"key\": \"value\"}";
        let hash = compute_hash(data);

        block_on(async {
            store.write(&hash, data.to_vec(), false).await.unwrap();
            let read_data = store.read(&hash, false).await.unwrap().unwrap();
            assert_eq!(read_data, data);
            assert!(store.read(&hash, true).await.unwrap().is_none());
        });
    }

    #[test]
    fn test_delete() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();
        let data = b"hello world";
        let hash = compute_hash(data);

        block_on(async {
            store.write(&hash, data.to_vec(), true).await.unwrap();
            let read_data = store.read(&hash, true).await.unwrap().unwrap();
            assert_eq!(read_data, data);

            store.delete(&hash, true).await.unwrap();
            let result = store.read(&hash, true).await.unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_enumerate_all() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();
        let data1 = b"data1";
        let data2 = b"data2";
        let hash1 = compute_hash(data1);
        let hash2 = compute_hash(data2);

        block_on(async {
            store.write(&hash1, data1.to_vec(), true).await.unwrap();
            store.write(&hash2, data2.to_vec(), false).await.unwrap();

            let all = store.enumerate_all().await.unwrap();
            assert_eq!(all.len(), 2);
            assert!(all.contains(&(hash1, true)));
            assert!(all.contains(&(hash2, false)));
        });
    }

    #[test]
    fn test_object_path() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();
        let hash = compute_hash(b"abcd");
        let hex = hash.to_string();

        // Test leaf path
        let leaf_path = store.object_path(&hash, true);
        assert_eq!(leaf_path, temp.join(DATA).join(&hex[0..2]).join(&hex[2..]));

        // Test non-leaf path
        let non_leaf_path = store.object_path(&hash, false);
        assert_eq!(non_leaf_path, temp.join(TREE).join(&hex));
    }
}
