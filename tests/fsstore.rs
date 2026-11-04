mod common;

use std::{collections::HashSet, fs, path::PathBuf};

use common::object_path;
use futures::executor::block_on;
use merkle_store::{ContentHash, TreeEnumerator, TreeReader, TreeWriter, fsstore::FsStore};

fn temp_dir() -> PathBuf {
    common::temp_dir("fsstore_test")
}

#[test]
fn test_fsstore_new() {
    let temp = temp_dir();
    FsStore::new(&temp).unwrap();
    assert!(temp.join("tree").exists());
    assert!(temp.join("data").exists());
}

#[test]
fn test_enumerate_skips_temp_files() {
    let temp = temp_dir();
    let store = FsStore::new(&temp).unwrap();
    let data = b"data";
    let hash = ContentHash::of(data);
    block_on(async {
        store.write(&hash, data.to_vec(), false).await.unwrap();
        store.write(&hash, data.to_vec(), true).await.unwrap();
        // Left behind by writes that were interrupted.
        let mut tmp = object_path(&temp, &hash, false).into_os_string();
        tmp.push(".tmp");
        fs::write(&tmp, b"partial").unwrap();
        let mut tmp = object_path(&temp, &hash, true).into_os_string();
        tmp.push(".tmp");
        fs::write(&tmp, b"partial").unwrap();

        let all = store.enumerate_all().await.unwrap();
        assert_eq!(all, HashSet::from([(hash, false), (hash, true)]));
    });
}

#[test]
fn test_write_read_data() {
    let temp = temp_dir();
    let store = FsStore::new(&temp).unwrap();
    let data = b"hello world";
    let hash = ContentHash::of(data);

    block_on(async {
        store.write(&hash, data.to_vec(), true).await.unwrap();
        let read_data = store.read(&hash, usize::MAX, true).await.unwrap().unwrap();
        assert_eq!(read_data, data);
        // The same hash in the other namespace is a different object.
        assert!(
            store
                .read(&hash, usize::MAX, false)
                .await
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn test_write_read_node() {
    let temp = temp_dir();
    let store = FsStore::new(&temp).unwrap();
    let data = b"{\"key\": \"value\"}";
    let hash = ContentHash::of(data);

    block_on(async {
        store.write(&hash, data.to_vec(), false).await.unwrap();
        let read_data = store.read(&hash, usize::MAX, false).await.unwrap().unwrap();
        assert_eq!(read_data, data);
        assert!(store.read(&hash, usize::MAX, true).await.unwrap().is_none());
    });
}

#[test]
fn test_delete() {
    let temp = temp_dir();
    let store = FsStore::new(&temp).unwrap();
    let data = b"hello world";
    let hash = ContentHash::of(data);

    block_on(async {
        store.write(&hash, data.to_vec(), true).await.unwrap();
        let read_data = store.read(&hash, usize::MAX, true).await.unwrap().unwrap();
        assert_eq!(read_data, data);

        store.delete(&hash, true).await.unwrap();
        let result = store.read(&hash, usize::MAX, true).await.unwrap();
        assert!(result.is_none());
    });
}

#[test]
fn test_enumerate_all() {
    let temp = temp_dir();
    let store = FsStore::new(&temp).unwrap();
    let data1 = b"data1";
    let data2 = b"data2";
    let hash1 = ContentHash::of(data1);
    let hash2 = ContentHash::of(data2);

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
fn test_object_layout() {
    let temp = temp_dir();
    let store = FsStore::new(&temp).unwrap();
    let data = b"abcd";
    let hash = ContentHash::of(data);
    // Files are named by the SHA-256 in hex.
    let hex = "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589";
    assert_eq!(hash.to_string(), hex);

    block_on(async {
        store.write(&hash, data.to_vec(), true).await.unwrap();
        store.write(&hash, data.to_vec(), false).await.unwrap();
    });
    // Data blobs: data/<first 2 hex>/<rest>. Tree nodes: tree/<hex>.
    assert_eq!(
        fs::read(temp.join("data").join(&hex[0..2]).join(&hex[2..])).unwrap(),
        data
    );
    assert_eq!(fs::read(temp.join("tree").join(hex)).unwrap(), data);
}
