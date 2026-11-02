use std::collections::HashSet;

use futures::{FutureExt, executor::block_on, lock::Mutex};
use merkletree::{
    ContentHash, MerkleStore, TreeEnumerator, TreeParameters, TreeReader, TreeWriter, compute_hash,
    memstore::MemoryStore,
};

fn store(depth: u8, breadth: u8) -> MerkleStore<MemoryStore> {
    MerkleStore::new(MemoryStore::new(), 4, TreeParameters { depth, breadth })
}

async fn get(store: &MerkleStore<impl TreeWriter>, name: &str) -> Option<Vec<u8>> {
    store.get_file(name).await.unwrap()
}

#[test]
fn second_commit_into_sparse_tree() {
    // Few files in a wide tree, so the second commit creates index nodes
    // under root slots that were empty.
    block_on(async {
        let store = store(2, 8);
        for i in 0..5u8 {
            store.put_object(&format!("f{i}"), vec![i]).await.unwrap();
        }
        store.commit().await.unwrap();
        for i in 5..10u8 {
            store.put_object(&format!("f{i}"), vec![i]).await.unwrap();
        }
        store.commit().await.unwrap();

        let mut names: Vec<_> = store
            .enumerate()
            .await
            .unwrap()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        names.sort();
        let expected: Vec<_> = (0..10).map(|i| format!("f{i}")).collect();
        assert_eq!(names, expected);
        for i in 0..10u8 {
            assert_eq!(get(&store, &format!("f{i}")).await, Some(vec![i]));
        }
    });
}

#[test]
fn put_replaces_existing_name() {
    for depth in [0, 2] {
        block_on(async {
            let store = store(depth, 4);
            store.put_object("a", b"one".to_vec()).await.unwrap();
            store.commit().await.unwrap();
            store.put_object("a", b"two".to_vec()).await.unwrap();
            store.commit().await.unwrap();
            assert_eq!(get(&store, "a").await, Some(b"two".to_vec()));

            // Within a single commit, the last put wins.
            store.put_object("a", b"three".to_vec()).await.unwrap();
            store.put_object("a", b"four".to_vec()).await.unwrap();
            store.commit().await.unwrap();
            assert_eq!(get(&store, "a").await, Some(b"four".to_vec()));
            assert_eq!(store.enumerate().await.unwrap().len(), 1);
        });
    }
}

#[test]
fn empty_file() {
    block_on(async {
        let store = store(2, 4);
        store.put_object("empty", vec![]).await.unwrap();
        store.commit().await.unwrap();
        assert_eq!(get(&store, "empty").await, Some(vec![]));
    });
}

#[test]
fn empty_hash_is_not_null() {
    assert!(!compute_hash(&[]).is_null());
}

/// A MemoryStore whose tree-node writes wait on `gate` and can be made to fail.
#[derive(Default)]
struct Gated {
    inner: MemoryStore,
    gate: Mutex<()>,
    fail: std::sync::atomic::AtomicBool,
}

impl TreeReader for Gated {
    type Error = &'static str;
    async fn read(
        &self,
        hash: &ContentHash,
        is_data: bool,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.inner.read(hash, is_data).await.unwrap())
    }
}

impl TreeWriter for Gated {
    async fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_data: bool,
    ) -> Result<(), Self::Error> {
        if !is_data {
            let _gate = self.gate.lock().await;
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("write failed");
            }
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
fn readers_are_not_blocked_by_commit() {
    block_on(async {
        let store = MerkleStore::new(
            Gated::default(),
            1,
            TreeParameters {
                depth: 2,
                breadth: 4,
            },
        );
        store.put_object("a", b"a".to_vec()).await.unwrap();
        store.commit().await.unwrap();

        store.put_object("b", b"b".to_vec()).await.unwrap();
        let gate = store.backend().gate.lock().await;
        let mut commit = Box::pin(store.commit());
        assert!(
            futures::poll!(&mut commit).is_pending(),
            "commit should wait on the gate"
        );

        // The old root is still readable while the commit is stuck.
        let a = store
            .get_file("a")
            .now_or_never()
            .expect("reader was blocked");
        assert_eq!(a.unwrap(), Some(b"a".to_vec()));
        assert_eq!(store.get_file("b").now_or_never().unwrap().unwrap(), None);

        drop(gate);
        commit.await.unwrap();
        assert_eq!(get(&store, "b").await, Some(b"b".to_vec()));
    });
}

#[test]
fn failed_commit_keeps_pending_writes() {
    block_on(async {
        let store = MerkleStore::new(
            Gated::default(),
            1,
            TreeParameters {
                depth: 2,
                breadth: 4,
            },
        );
        store.put_object("a", b"a".to_vec()).await.unwrap();
        store
            .backend()
            .fail
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(store.commit().await.is_err());
        assert!(!store.has_root().await);

        store
            .backend()
            .fail
            .store(false, std::sync::atomic::Ordering::SeqCst);
        store.commit().await.unwrap();
        assert_eq!(get(&store, "a").await, Some(b"a".to_vec()));
    });
}

#[test]
fn cancelled_commit_keeps_pending_writes() {
    block_on(async {
        let store = MerkleStore::new(
            Gated::default(),
            1,
            TreeParameters {
                depth: 2,
                breadth: 4,
            },
        );
        store.put_object("a", b"a".to_vec()).await.unwrap();
        {
            let _gate = store.backend().gate.lock().await;
            let mut commit = Box::pin(store.commit());
            assert!(futures::poll!(&mut commit).is_pending());
            // Dropping the commit future cancels it.
        }
        store.commit().await.unwrap();
        assert_eq!(get(&store, "a").await, Some(b"a".to_vec()));
    });
}

#[test]
fn gc_keeps_live_and_pending_objects() {
    block_on(async {
        let mut store = store(2, 4);
        store.put_object("a", b"old".to_vec()).await.unwrap();
        store.put_object("b", b"b".to_vec()).await.unwrap();
        store.commit().await.unwrap();
        store.put_object("a", b"new".to_vec()).await.unwrap();
        store.commit().await.unwrap();
        // Uploaded but not yet committed.
        store.put_object("c", b"c".to_vec()).await.unwrap();

        store.gc().await.unwrap();

        let blobs: HashSet<_> = store
            .backend()
            .enumerate_all()
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, is_data)| *is_data)
            .map(|(h, _)| h)
            .collect();
        assert!(!blobs.contains(&compute_hash(b"old")));
        assert!(blobs.contains(&compute_hash(b"c")));

        store.commit().await.unwrap();
        assert_eq!(get(&store, "a").await, Some(b"new".to_vec()));
        assert_eq!(get(&store, "b").await, Some(b"b".to_vec()));
        assert_eq!(get(&store, "c").await, Some(b"c".to_vec()));
    });
}

#[test]
fn test_memory_store() {
    futures::executor::block_on(async {
        let store = MerkleStore::new(
            MemoryStore::new(),
            1,
            TreeParameters {
                depth: 2,
                breadth: 4,
            },
        );

        for i in 0..100 {
            store
                .put_object(&format!("node:{i}"), format!("data:{i}").into_bytes())
                .await
                .unwrap();
        }
        store.commit().await.unwrap();

        for i in 100..200 {
            store
                .put_object(&format!("node:{i}"), format!("data:{i}").into_bytes())
                .await
                .unwrap();
        }

        store.commit().await.unwrap();

        for i in 0..200 {
            assert_eq!(
                store.get_file(&format!("node:{i}")).await.unwrap().unwrap(),
                format!("data:{i}").into_bytes()
            );
        }
    })
}

#[test]
fn multiple_files() {
    futures::executor::block_on(async {
        let store = MerkleStore::new(
            MemoryStore::new(),
            1,
            TreeParameters {
                depth: 2,
                breadth: 4,
            },
        );
        store.put_object("test", vec![1]).await.unwrap();
        store.commit().await.unwrap();
        let content = store.get_file("test").await.unwrap().unwrap();
        assert_eq!(content, [1]);
    })
}

async fn put_all(store: &MerkleStore<MemoryStore>, names: impl IntoIterator<Item = u8>) {
    for i in names {
        store.put_object(&format!("f{i}"), vec![i]).await.unwrap();
    }
}

#[test]
fn delete_removes_name() {
    block_on(async {
        let store = store(2, 4);
        store.put_object("a", b"a".to_vec()).await.unwrap();
        store.put_object("b", b"b".to_vec()).await.unwrap();
        store.commit().await.unwrap();

        store.delete("a").await.unwrap();
        // Not visible until commit.
        assert_eq!(get(&store, "a").await, Some(b"a".to_vec()));
        store.commit().await.unwrap();

        assert_eq!(get(&store, "a").await, None);
        assert_eq!(get(&store, "b").await, Some(b"b".to_vec()));
        let names: Vec<_> = store
            .enumerate()
            .await
            .unwrap()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(names, ["b"]);
    });
}

#[test]
fn last_put_or_delete_wins_within_a_commit() {
    block_on(async {
        let store = store(2, 4);
        store.put_object("a", b"a".to_vec()).await.unwrap();
        store.delete("a").await.unwrap();
        store.put_object("b", b"b".to_vec()).await.unwrap();
        store.commit().await.unwrap();
        assert_eq!(get(&store, "a").await, None);

        store.delete("b").await.unwrap();
        store.put_object("b", b"b2".to_vec()).await.unwrap();
        store.commit().await.unwrap();
        assert_eq!(get(&store, "b").await, Some(b"b2".to_vec()));
    });
}

#[test]
fn delete_missing_name_changes_nothing() {
    block_on(async {
        let store = store(2, 4);
        store.delete("nothing").await.unwrap();
        store.commit().await.unwrap();
        assert_eq!(store.root().await, None);

        store.put_object("a", b"a".to_vec()).await.unwrap();
        store.commit().await.unwrap();
        let root = store.root().await;
        store.delete("nothing").await.unwrap();
        store.commit().await.unwrap();
        assert_eq!(store.root().await, root);
    });
}

#[test]
fn deleting_everything_clears_root() {
    for depth in [0, 2] {
        block_on(async {
            let mut store = store(depth, 4);
            put_all(&store, 0..20).await;
            store.commit().await.unwrap();
            for i in 0..20 {
                store.delete(&format!("f{i}")).await.unwrap();
            }
            store.commit().await.unwrap();

            assert_eq!(store.root().await, None);
            assert!(store.enumerate().await.unwrap().is_empty());
            store.gc().await.unwrap();
            assert!(store.backend().enumerate_all().await.unwrap().is_empty());

            // The store is still usable afterwards.
            put_all(&store, [1]).await;
            store.commit().await.unwrap();
            assert_eq!(get(&store, "f1").await, Some(vec![1]));
        });
    }
}

#[test]
fn delete_gives_same_tree_as_never_adding() {
    // A wide tree with few files, so deletes empty out whole index nodes.
    block_on(async {
        let a = store(2, 8);
        put_all(&a, 0..10).await;
        a.commit().await.unwrap();
        for i in 0..5 {
            a.delete(&format!("f{i}")).await.unwrap();
        }
        a.commit().await.unwrap();

        let b = store(2, 8);
        put_all(&b, 5..10).await;
        b.commit().await.unwrap();

        assert_eq!(a.root().await, b.root().await);
    });
}

#[test]
fn gc_removes_deleted_data() {
    block_on(async {
        let mut store = store(2, 4);
        store.put_object("a", b"a".to_vec()).await.unwrap();
        store.put_object("b", b"b".to_vec()).await.unwrap();
        store.commit().await.unwrap();
        store.delete("a").await.unwrap();
        store.commit().await.unwrap();
        store.gc().await.unwrap();

        let all = store.backend().enumerate_all().await.unwrap();
        assert!(!all.contains(&(compute_hash(b"a"), true)));
        assert!(all.contains(&(compute_hash(b"b"), true)));
    });
}
