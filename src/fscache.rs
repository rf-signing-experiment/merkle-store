use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use tracing::{error, warn};

use crate::{TreeEnumerator, TreeReader, TreeWriter, compute_hash, fsutil, types::ContentHash};

/// A local-directory cache in front of another backend.
///
/// Reads are read-through for both nodes and data blobs. Writes always go to
/// the inner backend; only tree nodes are also cached locally, and only after
/// the inner write succeeds. Local cache errors are logged, never returned.
pub struct FsCache<B> {
    inner: B,
    root: PathBuf,
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

impl<B> FsCache<B> {
    pub fn new(root: impl AsRef<Path>, backend: B) -> IoResult<Self> {
        let root = root.as_ref().to_path_buf();
        fsutil::create_layout(&root)?;
        Ok(Self {
            root,
            inner: backend,
        })
    }

    pub fn inner(&self) -> &B {
        &self.inner
    }

    fn object_path(&self, hash: &ContentHash, is_data: bool) -> PathBuf {
        fsutil::object_path(&self.root, hash, is_data)
    }

    /// Writes into the local cache, logging any error.
    fn cache_write(&self, path: &Path, data: &[u8]) {
        if let Err(e) = fsutil::write_atomic(path, data) {
            error!("cache write {}: {e}", path.display());
        }
    }
}

impl<B: TreeReader> TreeReader for FsCache<B> {
    type Error = B::Error;

    async fn read(
        &self,
        hash: &ContentHash,
        is_data: bool,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        let path = self.object_path(hash, is_data);
        match std::fs::read(&path) {
            Ok(data) if compute_hash(&data) == *hash => return Ok(Some(data)),
            Ok(_) => {
                // Damaged entry. Drop it and refetch from the inner backend.
                warn!("cache entry {} is corrupt, refetching", path.display());
                let _ = std::fs::remove_file(&path);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => error!("cache read {}: {e}", path.display()),
        }
        let result = self.inner.read(hash, is_data).await;
        if let Ok(Some(data)) = &result {
            self.cache_write(&path, data);
        }
        result
    }
}

impl<B: TreeWriter> TreeWriter for FsCache<B> {
    async fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_data: bool,
    ) -> Result<(), Self::Error> {
        // Always forward the write: a local copy doesn't prove the inner
        // backend still has the object (it may have been garbage-collected
        // by another client). Skipping duplicates is the inner backend's job.
        if is_data {
            return self.inner.write(hash, data, true).await;
        }
        // Cache the node only once the inner write succeeded, so a failed
        // write is never mistaken for a stored one.
        self.inner.write(hash, data.clone(), false).await?;
        let path = self.object_path(hash, false);
        if !path.exists() {
            self.cache_write(&path, &data);
        }
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash, is_data: bool) -> Result<(), Self::Error> {
        let path = self.object_path(hash, is_data);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => error!("cache delete {}: {e}", path.display()),
        }
        self.inner.delete(hash, is_data).await
    }
}

impl<B: TreeEnumerator> TreeEnumerator for FsCache<B> {
    async fn enumerate_all(&self) -> Result<HashSet<(ContentHash, bool)>, Self::Error> {
        self.inner.enumerate_all().await
    }
}
