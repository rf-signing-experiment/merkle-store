mod common;

use std::sync::atomic::{AtomicBool, Ordering};

use common::{object_path, temp_dir};
use futures::executor::block_on;
use merkletree::{
    ContentHash, TreeReader, TreeWriter, compute_hash, fscache::FsCache, memstore::MemoryStore,
};

/// A MemoryStore whose writes fail while `fail` is set.
#[derive(Default)]
struct Flaky {
    inner: MemoryStore,
    fail: AtomicBool,
}

impl TreeReader for Flaky {
    type Error = &'static str;
    async fn read(
        &self,
        hash: &ContentHash,
        is_data: bool,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.inner.read(hash, is_data).await.unwrap())
    }
}

impl TreeWriter for Flaky {
    async fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_data: bool,
    ) -> Result<(), Self::Error> {
        if self.fail.load(Ordering::SeqCst) {
            return Err("write failed");
        }
        self.inner.write(hash, data, is_data).await.unwrap();
        Ok(())
    }
    async fn delete(&self, hash: &ContentHash, is_data: bool) -> Result<(), Self::Error> {
        self.inner.delete(hash, is_data).await.unwrap();
        Ok(())
    }
}

#[test]
fn failed_node_write_is_retried() {
    let dir = temp_dir("fscache_retry");
    let cache = FsCache::new(&dir, Flaky::default()).unwrap();
    let data = b"node".to_vec();
    let hash = compute_hash(&data);
    block_on(async {
        cache.inner().fail.store(true, Ordering::SeqCst);
        assert!(cache.write(&hash, data.clone(), false).await.is_err());
        assert!(!object_path(&dir, &hash, false).exists());

        cache.inner().fail.store(false, Ordering::SeqCst);
        cache.write(&hash, data.clone(), false).await.unwrap();
        let stored = cache.inner().inner.read(&hash, false).await.unwrap();
        assert_eq!(stored, Some(data));
    });
}

#[test]
fn write_reaches_inner_even_if_cached_locally() {
    let dir = temp_dir("fscache_rewrite");
    let cache = FsCache::new(&dir, MemoryStore::new()).unwrap();
    let data = b"blob".to_vec();
    let hash = compute_hash(&data);
    block_on(async {
        cache.write(&hash, data.clone(), true).await.unwrap();
        // Read-through puts it in the local cache.
        assert_eq!(cache.read(&hash, true).await.unwrap(), Some(data.clone()));
        assert!(object_path(&dir, &hash, true).exists());

        // Another client deletes it from the shared backend, then we write it again.
        cache.inner().delete(&hash, true).await.unwrap();
        cache.write(&hash, data.clone(), true).await.unwrap();
        assert_eq!(cache.inner().read(&hash, true).await.unwrap(), Some(data));
    });
}

#[test]
fn corrupt_cache_entry_is_refetched() {
    let dir = temp_dir("fscache_corrupt");
    let cache = FsCache::new(&dir, MemoryStore::new()).unwrap();
    let data = b"node".to_vec();
    let hash = compute_hash(&data);
    block_on(async {
        cache.write(&hash, data.clone(), false).await.unwrap();
        let path = object_path(&dir, &hash, false);
        std::fs::write(&path, b"no").unwrap();

        assert_eq!(cache.read(&hash, false).await.unwrap(), Some(data.clone()));
        assert_eq!(std::fs::read(&path).unwrap(), data);
    });
}
