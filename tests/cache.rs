mod common;

use std::sync::atomic::{AtomicBool, Ordering};

use common::{object_path, temp_dir};
use futures::executor::block_on;
use merkle_store::{
    ContentHash, MerkleStore, TreeParameters, TreeReader, TreeWriter,
    cache::{Cache, FsCache},
    compute_hash,
    fsstore::FsStore,
    memstore::MemoryStore,
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

fn mem_cache<R>(remote: R) -> Cache<MemoryStore, R> {
    Cache::new(MemoryStore::new(), remote)
}

#[test]
fn failed_node_write_is_retried() {
    let cache = mem_cache(Flaky::default());
    let data = b"node".to_vec();
    let hash = compute_hash(&data);
    block_on(async {
        cache.remote().fail.store(true, Ordering::SeqCst);
        assert!(cache.write(&hash, data.clone(), false).await.is_err());
        assert_eq!(cache.local().read(&hash, false).await.unwrap(), None);

        cache.remote().fail.store(false, Ordering::SeqCst);
        cache.write(&hash, data.clone(), false).await.unwrap();
        let stored = cache.remote().inner.read(&hash, false).await.unwrap();
        assert_eq!(stored, Some(data));
    });
}

#[test]
fn only_nodes_are_cached_on_write() {
    let cache = mem_cache(MemoryStore::new());
    let node = b"node".to_vec();
    let blob = b"blob".to_vec();
    block_on(async {
        cache
            .write(&compute_hash(&node), node.clone(), false)
            .await
            .unwrap();
        cache
            .write(&compute_hash(&blob), blob.clone(), true)
            .await
            .unwrap();
        let local = cache.local();
        assert_eq!(
            local.read(&compute_hash(&node), false).await.unwrap(),
            Some(node)
        );
        assert_eq!(local.read(&compute_hash(&blob), true).await.unwrap(), None);

        // Reading a blob copies it into the cache.
        cache.read(&compute_hash(&blob), true).await.unwrap();
        assert_eq!(
            local.read(&compute_hash(&blob), true).await.unwrap(),
            Some(blob)
        );
    });
}

#[test]
fn write_reaches_remote_even_if_cached_locally() {
    let cache = mem_cache(MemoryStore::new());
    let data = b"blob".to_vec();
    let hash = compute_hash(&data);
    block_on(async {
        cache.write(&hash, data.clone(), true).await.unwrap();
        // Read-through puts it in the local cache.
        assert_eq!(cache.read(&hash, true).await.unwrap(), Some(data.clone()));

        // Another client deletes it from the remote, then we write it again.
        cache.remote().delete(&hash, true).await.unwrap();
        cache.write(&hash, data.clone(), true).await.unwrap();
        assert_eq!(cache.remote().read(&hash, true).await.unwrap(), Some(data));
    });
}

#[test]
fn corrupt_cache_entry_is_refetched() {
    let cache = mem_cache(MemoryStore::new());
    let data = b"node".to_vec();
    let hash = compute_hash(&data);
    block_on(async {
        cache
            .remote()
            .write(&hash, data.clone(), false)
            .await
            .unwrap();
        cache
            .local()
            .write(&hash, b"no".to_vec(), false)
            .await
            .unwrap();

        assert_eq!(cache.read(&hash, false).await.unwrap(), Some(data.clone()));
        assert_eq!(cache.local().read(&hash, false).await.unwrap(), Some(data));
    });
}

#[test]
fn fs_cache_store_end_to_end() {
    let dir = temp_dir("fscache_store");
    let cache: FsCache<_> = Cache::new(FsStore::new(&dir).unwrap(), MemoryStore::new());
    let mut store = MerkleStore::new(
        cache,
        4,
        TreeParameters {
            depth: 2,
            breadth: 4,
        },
    );
    block_on(async {
        store.put_object("a", b"old".to_vec()).await.unwrap();
        store.commit().await.unwrap();
        store.put_object("a", b"new".to_vec()).await.unwrap();
        store.commit().await.unwrap();

        // The root node was cached in the local directory.
        let root = store.root().unwrap();
        assert!(object_path(&dir, &root, false).exists());

        // GC deletes through the cache, including objects it never cached.
        store.gc().await.unwrap();
        assert_eq!(store.get_file("a").await.unwrap(), Some(b"new".to_vec()));
        let old = compute_hash(b"old");
        assert_eq!(
            store.backend().remote().read(&old, true).await.unwrap(),
            None
        );
    });
}
