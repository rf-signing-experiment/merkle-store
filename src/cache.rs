use std::collections::HashSet;

use tracing::{error, warn};

use crate::{TreeEnumerator, TreeReader, TreeWriter, fsstore::FsStore, types::ContentHash};

/// A cache backed by a local directory.
pub type FsCache<R> = Cache<FsStore, R>;

/// A cache: backend `L` (local) in front of backend `R` (remote).
///
/// Reads try `local` first and fall back to `remote`, copying what they find
/// into `local`. Writes always go to `remote`; tree nodes are also copied
/// into `local`, but only after the remote write succeeds. Errors from
/// `local` are logged, never returned, so the cache can only make things
/// faster, not fail them.
pub struct Cache<L, R> {
    local: L,
    remote: R,
}

impl<L, R> Cache<L, R> {
    pub fn new(local: L, remote: R) -> Self {
        Self { local, remote }
    }

    pub fn local(&self) -> &L {
        &self.local
    }

    pub fn remote(&self) -> &R {
        &self.remote
    }
}

impl<L: TreeWriter, R: TreeReader> Cache<L, R> {
    async fn cache_write(&self, hash: &ContentHash, data: Vec<u8>, is_data: bool) {
        if let Err(e) = self.local.write(hash, data, is_data).await {
            error!("cache write {hash}: {e}");
        }
    }
}

impl<L: TreeWriter, R: TreeReader> TreeReader for Cache<L, R> {
    type Error = R::Error;

    async fn read(
        &self,
        hash: &ContentHash,
        size_limit: usize,
        is_data: bool,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        match self.local.read(hash, size_limit, is_data).await {
            Ok(Some(data)) if ContentHash::of(&data) == *hash => return Ok(Some(data)),
            Ok(Some(_)) => {
                // Damaged entry. Drop it and refetch from the remote.
                warn!("cache entry {hash} is corrupt, refetching");
                if let Err(e) = self.local.delete(hash, is_data).await {
                    error!("cache delete {hash}: {e}");
                }
            }
            Ok(None) => {}
            Err(e) => error!("cache read {hash}: {e}"),
        }
        let data = self.remote.read(hash, size_limit, is_data).await?;
        if let Some(data) = &data {
            self.cache_write(hash, data.clone(), is_data).await;
        }
        Ok(data)
    }
}

impl<L: TreeWriter, R: TreeWriter> TreeWriter for Cache<L, R> {
    async fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_data: bool,
    ) -> Result<(), Self::Error> {
        // Always forward the write: a local copy doesn't prove the remote
        // still has the object (another client may have garbage-collected
        // it). Skipping duplicates is the remote's job.
        if is_data {
            return self.remote.write(hash, data, true).await;
        }
        // Cache the node only once the remote write succeeded, so a failed
        // write is never mistaken for a stored one.
        self.remote.write(hash, data.clone(), false).await?;
        self.cache_write(hash, data, false).await;
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash, is_data: bool) -> Result<(), Self::Error> {
        if let Err(e) = self.local.delete(hash, is_data).await {
            error!("cache delete {hash}: {e}");
        }
        self.remote.delete(hash, is_data).await
    }
}

impl<L: TreeWriter, R: TreeEnumerator> TreeEnumerator for Cache<L, R> {
    async fn enumerate_all(&self) -> Result<HashSet<(ContentHash, bool)>, Self::Error> {
        self.remote.enumerate_all().await
    }
}
