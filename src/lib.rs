pub mod fscache;
pub mod fsstore;
pub mod memstore;
pub mod oneflight;
mod types;

use futures::{
    StreamExt, TryStreamExt,
    lock::Mutex,
    stream::{self, FuturesUnordered},
};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    fmt::Display,
    future::Future,
    sync::Arc,
};
use thiserror::Error;
use tracing::{debug, trace};

use crate::oneflight::OneFlightCache;
use crate::types::{IndexNode, LeafNode, Node};
type MerkleResult<T, E> = std::result::Result<T, Error<E>>;
use types::NameHash;

pub use crate::types::ContentHash;

#[derive(Clone)]
pub struct TreeParameters {
    pub depth: u8,
    pub breadth: u8,
}

#[derive(Debug, Error)]
pub enum Error<T>
where
    T: Display + Debug + 'static,
{
    #[error("hash `{hash}` should be present, but was missing")]
    NotFound { hash: ContentHash },
    #[error("hash mismatch: expected {expected}, actual {actual}")]
    HashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error(transparent)]
    Backend(#[from] T),
}

pub trait TreeReader {
    type Error: Display + Debug + 'static;

    /// Read data by hash.
    fn read(
        &self,
        hash: &ContentHash,
        is_data: bool,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>>;
}

pub trait TreeWriter: TreeReader {
    /// Write data into the store with the given hash.
    fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_data: bool,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Delete an object.
    fn delete(
        &self,
        hash: &ContentHash,
        is_data: bool,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

pub trait TreeEnumerator: TreeWriter {
    /// Enumerate all hashes in the store (regardless of reachability).
    fn enumerate_all(
        &self,
    ) -> impl std::future::Future<Output = Result<HashSet<(ContentHash, bool)>, Self::Error>>;
}

pub struct RoMerkleStore<B: TreeReader> {
    backend: B,
    parallel: usize,
    node_cache: OneFlightCache<ContentHash, Node>,
    root: Mutex<Option<ContentHash>>,
}

pub struct RwMerkleStore<B: TreeWriter> {
    inner: RoMerkleStore<B>,
    params: TreeParameters,
    pending_writes: std::sync::Mutex<Vec<(String, ContentHash)>>,
}

impl<B: TreeReader> RoMerkleStore<B> {
    pub fn new(backend: B, parallel: usize) -> Self {
        Self {
            backend,
            parallel,
            node_cache: OneFlightCache::new(),
            root: Mutex::new(None),
        }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub async fn has_root(&self) -> bool {
        self.root.lock().await.is_some()
    }

    pub async fn root(&self) -> Option<ContentHash> {
        self.root.lock().await.clone()
    }

    pub async fn set_root(&self, root: ContentHash) {
        *self.root.lock().await = Some(root);
    }

    pub async fn get_file(&self, name: &str) -> MerkleResult<Option<Vec<u8>>, B::Error> {
        match self.get_file_hash(name).await? {
            Some(hash) => Ok(Some(self.get_file_by_hash(&hash).await?)),
            None => Ok(None),
        }
    }

    pub async fn get_file_by_hash(&self, hash: &ContentHash) -> MerkleResult<Vec<u8>, B::Error> {
        Ok(self.read_object(hash, true).await?)
    }

    pub async fn get_file_hash(&self, name: &str) -> MerkleResult<Option<ContentHash>, B::Error> {
        trace!(?name, "get_file_hash");
        let key = NameHash::new(name, 32);
        let hash = self
            .lookup(self.root().await.as_ref(), key)
            .await?
            .as_ref()
            .and_then(|node| node.as_leaf())
            .and_then(|node| node.get(name))
            .cloned();
        Ok(hash)
    }

    // Reads from the store by hash.
    async fn read_object(
        &self,
        hash: &ContentHash,
        is_data: bool,
    ) -> MerkleResult<Vec<u8>, B::Error> {
        let data = self
            .backend
            .read(&hash, is_data)
            .await?
            .ok_or_else(|| Error::NotFound { hash: hash.clone() })?;
        let verification_hash = compute_hash(&data);
        if hash != &verification_hash {
            return Err(Error::HashMismatch {
                expected: hash.clone(),
                actual: verification_hash,
            });
        }
        Ok(data)
    }

    async fn lookup(
        &self,
        mut root: Option<&ContentHash>,
        mut key: NameHash,
    ) -> MerkleResult<Option<Arc<Node>>, B::Error> {
        debug!(?key, "lookup");
        let mut node = None;
        while let Some(h) = root {
            node = Some(self.load_node(&h).await?);
            match node.as_deref().unwrap() {
                Node::Index(index_node) => {
                    if key.is_empty() {
                        return Ok(node);
                    }
                    let (remaining, index) = key.split(index_node.len_bits());
                    key = remaining;
                    root = index_node.get(index);
                }
                Node::Leaf(..) => return Ok(node),
            }
        }
        Ok(node)
    }

    async fn load_node_uncached(&self, hash: &ContentHash) -> MerkleResult<Node, B::Error> {
        let data = self.read_object(hash, false).await?;
        let node = Node::deserialize(data);
        trace!("load node {hash}\n{node:#?}");
        Ok(node)
    }

    async fn load_node(&self, hash: &ContentHash) -> MerkleResult<Arc<Node>, B::Error> {
        self.node_cache
            .get(hash, async |hash| self.load_node_uncached(hash).await)
            .await
    }
}

impl<B: TreeWriter> RwMerkleStore<B> {
    pub fn new(backend: B, parallel: usize, params: TreeParameters) -> Self {
        Self {
            inner: RoMerkleStore::new(backend, parallel),
            params,
            pending_writes: Default::default(),
        }
    }

    pub fn backend(&self) -> &B {
        &self.inner.backend()
    }

    pub async fn has_root(&self) -> bool {
        self.inner.has_root().await
    }

    pub async fn root(&self) -> Option<ContentHash> {
        self.inner.root().await
    }

    pub async fn set_root(&self, root: ContentHash) {
        self.inner.set_root(root).await
    }

    pub async fn get_file(&self, name: &str) -> MerkleResult<Option<Vec<u8>>, B::Error> {
        self.inner.get_file(name).await
    }

    pub async fn get_file_by_hash(&self, hash: &ContentHash) -> MerkleResult<Vec<u8>, B::Error> {
        self.inner.get_file_by_hash(hash).await
    }

    pub async fn get_file_hash(&self, name: &str) -> MerkleResult<Option<ContentHash>, B::Error> {
        self.inner.get_file_hash(name).await
    }

    pub async fn delete(&self, _name: &str) -> MerkleResult<(), B::Error> {
        // Not implemented yet
        Ok(())
    }

    // Saves an object into the store, replacing if the name was already used.
    pub async fn put_object(&self, name: &str, data: Vec<u8>) -> MerkleResult<(), B::Error> {
        let hash = self.write_object(data, true).await?;
        self.pending_writes
            .lock()
            .unwrap()
            .push((name.to_string(), hash));
        Ok(())
    }

    /// Commits all pending writes.
    pub async fn commit(&self) -> MerkleResult<(), B::Error> {
        debug!("commit:starting");
        // Hold the lock to prevent dropped writes.
        let mut root_lock = self.inner.root.lock().await;
        let root = root_lock.clone();
        let tree_depth = self.params.depth;
        let tree_breadth = self.params.breadth;

        // Group all the pending writes into buckets by hash.
        let pending = std::mem::take(&mut *self.pending_writes.lock().unwrap());
        debug!(pending = pending.len(), "commit:grouping");
        let mut premap: HashMap<NameHash, Vec<(String, ContentHash)>> = HashMap::new();
        for (name, hash) in pending {
            let key = NameHash::new(&name, tree_depth * tree_breadth);
            premap.entry(key).or_default().push((name, hash))
        }

        debug!(pending = premap.len(), "commit:writing leaves");
        let mut map: HashMap<NameHash, Vec<(NameHash, ContentHash)>> = HashMap::new();
        let mut futures = stream::iter(premap.into_iter().map(|(key, v)| async move {
            // Find an existing leaf node to merge with.
            let node = self.inner.lookup(root.as_ref(), key).await?;
            let mut node = node
                .as_ref()
                .and_then(|node| node.as_leaf())
                .cloned()
                .unwrap_or_else(|| LeafNode::new());
            for (key, value) in v {
                node.insert(key, value);
            }
            let hash = self.write_node(&Node::Leaf(node)).await?;
            MerkleResult::Ok((key, hash))
        }))
        .buffer_unordered(self.inner.parallel);
        while let Some((key, hash)) = futures.try_next().await? {
            if tree_depth == 0 {
                *root_lock = Some(hash);
                return Ok(());
            }
            let (left, right) = key.split(tree_breadth * (tree_depth - 1));
            map.entry(right).or_default().push((left, hash));
        }

        let mut next_root = root;
        for depth in (0..tree_depth).rev() {
            debug!(pending = map.len(), depth, "commit:writing tree");
            let mut next_map: HashMap<NameHash, Vec<_>> = HashMap::new();
            let mut futures = stream::iter(map.into_iter().map(|(key, v)| async move {
                trace!("writing into node {key:?}");
                let node = self.inner.lookup(root.as_ref(), key).await?;
                let mut node = node
                    .as_ref()
                    .and_then(|n| n.as_index())
                    .cloned()
                    .unwrap_or_else(|| IndexNode::new(tree_breadth));

                for (index, value) in v {
                    node.insert(index, value);
                }

                let hash = self.write_node(&Node::Index(node)).await?;
                MerkleResult::Ok((key, hash))
            }))
            .buffer_unordered(self.inner.parallel);
            while let Some((key, hash)) = futures.try_next().await? {
                next_root = Some(hash);
                if depth > 0 {
                    let (left, right) = key.split(tree_breadth * (depth - 1));
                    next_map.entry(right).or_default().push((left, hash));
                }
            }

            map = next_map;
        }

        *root_lock = next_root;
        Ok(())
    }

    /// Hashes the data and writes into the store.
    /// Returns the object hash.
    async fn write_object(
        &self,
        data: Vec<u8>,
        is_data: bool,
    ) -> MerkleResult<ContentHash, B::Error> {
        let hash = compute_hash(&data);
        trace!(hash = ?hash, "write");
        self.inner.backend.write(&hash, data, is_data).await?;
        Ok(hash)
    }

    async fn write_node(&self, node: &Node) -> MerkleResult<ContentHash, B::Error> {
        trace!(?node, "write_node");
        let data = node.serialize();
        self.write_object(data, false).await
    }
}

impl<B: TreeEnumerator> RoMerkleStore<B> {
    async fn enumerate_live_objects(&self) -> MerkleResult<HashSet<(ContentHash, bool)>, B::Error> {
        let mut live = HashSet::new();
        if let Some(head) = &self.root().await {
            // Takes the content hash by value
            let load_node = async |hash: ContentHash| self.load_node(&hash).await;
            let mut pending = FuturesUnordered::new();
            // Insert the root to get started
            pending.push(load_node(*head));
            live.insert((head.clone(), false));
            while let Some(node) = pending.try_next().await? {
                match &*node {
                    Node::Index(index_node) => {
                        for hash in index_node.iter() {
                            if live.insert((hash.clone(), false)) {
                                let hash = hash.clone();
                                pending.push(load_node(hash));
                            }
                        }
                    }
                    Node::Leaf(leaf_node) => {
                        for (name, hash) in leaf_node.iter() {
                            trace!(name, ?hash);
                            live.insert((hash.clone(), true));
                        }
                    }
                }
            }
        };

        Ok(live)
    }

    pub async fn enumerate(&self) -> MerkleResult<Vec<(String, ContentHash)>, B::Error> {
        let mut live = Vec::new();
        if let Some(head) = &self.root().await {
            // Takes the content hash by value
            let load_node = async |hash: ContentHash| self.load_node(&hash).await;
            let mut pending = FuturesUnordered::new();
            // Insert the root to get started
            pending.push(load_node(*head));
            while let Some(node) = pending.try_next().await? {
                match &*node {
                    Node::Index(index_node) => {
                        for hash in index_node.iter() {
                            let hash = hash.clone();
                            pending.push(load_node(hash));
                        }
                    }
                    Node::Leaf(leaf_node) => {
                        live.extend(leaf_node.iter().map(|(k, v)| (k.clone(), v.clone())));
                    }
                }
            }
        };

        Ok(live)
    }
}

impl<B: TreeEnumerator> RwMerkleStore<B> {
    pub async fn enumerate(&self) -> MerkleResult<Vec<(String, ContentHash)>, B::Error> {
        self.inner.enumerate().await
    }

    pub async fn gc(&self) -> MerkleResult<(), B::Error> {
        debug!("starting GC");

        let all = self.inner.backend.enumerate_all().await?;
        debug!(all = all.len(), "all objects enumerated");

        let live = self.inner.enumerate_live_objects().await?;
        debug!(live = live.len(), "live objects enumerated");

        let dead: Vec<_> = all.difference(&live).cloned().collect();
        debug!(dead = dead.len(), "GC completed");

        for (entry, is_data) in &dead {
            trace!(hash = ?entry, is_data, "deleted");
            self.inner.backend.delete(entry, *is_data).await?
        }
        debug!("GC delete completed");

        Ok(())
    }
}

fn compute_hash(data: &[u8]) -> ContentHash {
    let len: u8 = (data.len().ilog2() + 1).try_into().unwrap();
    let mut out = [0u8; 33];
    out[0] = len;
    let output: [u8; 32] = Sha256::digest(data).into();
    out[1..].copy_from_slice(&output);
    out.into()
}
