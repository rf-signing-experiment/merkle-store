pub mod cache;
pub mod fsstore;
pub mod memstore;
pub mod oneflight;
mod types;

use futures::{
    StreamExt, TryStreamExt,
    lock::Mutex,
    stream::{self, FuturesUnordered},
};
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
use crate::types::{IndexNode, Node};
type MerkleResult<T, E> = std::result::Result<T, Error<E>>;
use types::NameHash;

pub use crate::types::{ContentHash, SizedHash};

#[derive(Clone, Copy)]
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

/// Read access to a backend.
///
/// Every method takes an `is_data` flag: `true` for file contents (data
/// blobs), `false` for serialized tree nodes. Backends keep the two in
/// separate namespaces, so the same hash can exist in both.
pub trait TreeReader {
    type Error: Display + Debug + 'static;

    /// Reads an object by hash. Returns `None` if it doesn't exist.
    ///
    /// `size_limit` is the most bytes the object can have, from the size
    /// class the tree records next to its hash. A backend can use it to avoid
    /// reading more than that (say, from a remote that returns too much). The
    /// store checks the hash of whatever is returned, so it may ignore it.
    fn read(
        &self,
        hash: &ContentHash,
        size_limit: usize,
        is_data: bool,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>>;
}

pub trait TreeWriter: TreeReader {
    /// Writes an object under the given hash. Writing an object that
    /// already exists is not an error.
    fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_data: bool,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Deletes an object. Deleting a missing object is not an error.
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

/// A Merkle tree of named files, stored through backend `B`.
///
/// Reading needs `B: TreeReader`, writing needs `B: TreeWriter`, and GC needs
/// `B: TreeEnumerator`.
pub struct MerkleStore<B> {
    backend: B,
    parallel: usize,
    params: TreeParameters,
    node_cache: OneFlightCache<ContentHash, Node>,
    root: std::sync::Mutex<Option<SizedHash>>,
    pending_writes: std::sync::Mutex<Vec<PendingWrite>>,
    /// Serializes commits and `set_root`. The root lock itself is only held
    /// for a moment to read or swap the root, so readers never wait on a commit.
    commit_lock: Mutex<()>,
}

/// A queued change to a name: `Some(hash)` to put, `None` to delete.
type PendingWrite = (String, Option<SizedHash>);

impl<B: TreeReader> MerkleStore<B> {
    /// Creates a store with no root. `parallel` caps concurrent backend
    /// writes per tree level during `commit`. `params` sets the tree shape
    /// for writes; it must match the shape of any root passed to `set_root`.
    pub fn new(backend: B, parallel: usize, params: TreeParameters) -> Self {
        Self {
            backend,
            parallel,
            params,
            node_cache: OneFlightCache::new(),
            root: Default::default(),
            pending_writes: Default::default(),
            commit_lock: Mutex::new(()),
        }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// The current root, or `None` for an empty tree.
    pub fn root(&self) -> Option<SizedHash> {
        *self.root.lock().unwrap()
    }

    /// Replaces the root. Waits for any running commit to finish first, so
    /// the commit can't overwrite it.
    pub async fn set_root(&self, root: SizedHash) {
        let _commit = self.commit_lock.lock().await;
        *self.root.lock().unwrap() = Some(root);
    }

    pub async fn get_file(&self, name: &str) -> MerkleResult<Option<Vec<u8>>, B::Error> {
        match self.get_file_hash(name).await? {
            Some(hash) => Ok(Some(self.get_file_by_hash(&hash).await?)),
            None => Ok(None),
        }
    }

    pub async fn get_file_by_hash(&self, hash: &SizedHash) -> MerkleResult<Vec<u8>, B::Error> {
        self.read_object(hash, true).await
    }

    pub async fn get_file_hash(&self, name: &str) -> MerkleResult<Option<SizedHash>, B::Error> {
        trace!(?name, "get_file_hash");
        let key = NameHash::new(name, 32);
        let hash = self
            .lookup(self.root().as_ref(), key)
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
        hash: &SizedHash,
        is_data: bool,
    ) -> MerkleResult<Vec<u8>, B::Error> {
        let expected = *hash.hash();
        let data = self
            .backend
            .read(&expected, hash.size_limit(), is_data)
            .await?
            .ok_or(Error::NotFound { hash: expected })?;
        let actual = ContentHash::of(&data);
        if actual != expected {
            return Err(Error::HashMismatch { expected, actual });
        }
        Ok(data)
    }

    /// Walks from `root` along `key` and returns the node where the key runs
    /// out (or the first leaf). Returns `None` if the path hits an empty slot.
    async fn lookup(
        &self,
        root: Option<&SizedHash>,
        mut key: NameHash,
    ) -> MerkleResult<Option<Arc<Node>>, B::Error> {
        debug!(?key, "lookup");
        let Some(mut hash) = root.copied() else {
            return Ok(None);
        };
        loop {
            let node = self.load_node(&hash).await?;
            match &*node {
                Node::Index(index_node) => {
                    if key.is_empty() {
                        return Ok(Some(node));
                    }
                    let (remaining, index) = key.split(index_node.len_bits());
                    key = remaining;
                    match index_node.get(index) {
                        Some(child) => hash = *child,
                        None => return Ok(None),
                    }
                }
                Node::Leaf(..) => return Ok(Some(node)),
            }
        }
    }

    async fn load_node_uncached(&self, hash: &SizedHash) -> MerkleResult<Node, B::Error> {
        let data = self.read_object(hash, false).await?;
        let node = Node::deserialize(data);
        trace!("load node {hash}\n{node:#?}");
        Ok(node)
    }

    async fn load_node(&self, hash: &SizedHash) -> MerkleResult<Arc<Node>, B::Error> {
        // Keyed by the plain hash: it alone determines the node.
        self.node_cache
            .get(hash.hash(), async |_| self.load_node_uncached(hash).await)
            .await
    }
}

impl<B: TreeWriter> MerkleStore<B> {
    /// Removes `name` from the tree at the next `commit`. Deleting a name
    /// that doesn't exist is not an error. The file's data stays in the
    /// backend until `gc` runs.
    pub async fn delete(&self, name: &str) -> MerkleResult<(), B::Error> {
        self.pending_writes
            .lock()
            .unwrap()
            .push((name.to_string(), None));
        Ok(())
    }

    /// Saves an object into the store, replacing it if the name was already
    /// used. The data is written right away; the name appears in the tree at
    /// the next `commit`.
    pub async fn put_object(&self, name: &str, data: Vec<u8>) -> MerkleResult<(), B::Error> {
        let hash = self.write_object(data, true).await?;
        self.pending_writes
            .lock()
            .unwrap()
            .push((name.to_string(), Some(hash)));
        Ok(())
    }

    /// Commits all pending puts and deletes.
    ///
    /// If the commit fails or is cancelled, the pending writes stay queued
    /// for the next commit. Readers keep seeing the old root until the end.
    pub async fn commit(&self) -> MerkleResult<(), B::Error> {
        debug!("commit:starting");
        let _commit = self.commit_lock.lock().await;
        // Work from a copy. Puts and deletes only append to the queue and
        // commits run one at a time, so these stay the first `count` entries.
        let pending = self.pending_writes.lock().unwrap().clone();
        let count = pending.len();
        let next_root = self.build_tree(self.root(), pending).await?;
        // No await from here on, so cancellation can't split these two steps.
        *self.root.lock().unwrap() = next_root;
        self.pending_writes.lock().unwrap().drain(..count);
        Ok(())
    }

    /// Applies `pending` on top of `root` and returns the new root hash.
    ///
    /// Nodes left empty by deletes are dropped (their parent slot is
    /// cleared), so the result is the same tree as if the deleted names had
    /// never been added. If nothing is left, the new root is `None`.
    async fn build_tree(
        &self,
        root: Option<SizedHash>,
        pending: Vec<PendingWrite>,
    ) -> MerkleResult<Option<SizedHash>, B::Error> {
        let TreeParameters { depth, breadth } = self.params;

        // Group the edits by leaf. Order within a leaf is kept, so the last
        // put or delete of a name wins.
        let mut leaves: HashMap<NameHash, Vec<PendingWrite>> = HashMap::new();
        for (name, hash) in pending {
            let key = NameHash::new(&name, depth * breadth);
            leaves.entry(key).or_default().push((name, hash));
        }
        let mut level = self
            .write_level(root, leaves, |old, edits| {
                let mut leaf = old.and_then(Node::as_leaf).cloned().unwrap_or_default();
                for (name, hash) in edits {
                    match hash {
                        Some(hash) => leaf.insert(name, hash),
                        None => leaf.remove(&name),
                    }
                }
                Node::Leaf(leaf)
            })
            .await?;

        // Index levels, bottom-up. Each pass groups the new hashes from the
        // level below by parent and rewrites those parents.
        for d in (0..depth).rev() {
            let mut parents: HashMap<NameHash, Vec<(NameHash, Option<SizedHash>)>> = HashMap::new();
            for (key, hash) in level {
                let (slot, parent) = key.split(breadth * d);
                parents.entry(parent).or_default().push((slot, hash));
            }
            level = self
                .write_level(root, parents, |old, edits| {
                    let mut index = old
                        .and_then(Node::as_index)
                        .cloned()
                        .unwrap_or_else(|| IndexNode::new(breadth));
                    for (slot, hash) in edits {
                        match hash {
                            Some(hash) => index.insert(slot, hash),
                            None => index.remove(slot),
                        }
                    }
                    Node::Index(index)
                })
                .await?;
        }

        // What's left is the root (its key has no bits). If nothing was
        // pending, nothing was rewritten and the root is unchanged.
        Ok(level.into_values().next().unwrap_or(root))
    }

    /// Rewrites the nodes at `edits`' keys: loads each current node (if
    /// any), applies `edit`, and writes the result, up to `parallel` at a
    /// time. Returns each key's new hash, or `None` if the node ended up empty.
    async fn write_level<E>(
        &self,
        root: Option<SizedHash>,
        edits: HashMap<NameHash, E>,
        edit: impl Fn(Option<&Node>, E) -> Node,
    ) -> MerkleResult<HashMap<NameHash, Option<SizedHash>>, B::Error> {
        debug!(nodes = edits.len(), "commit:writing level");
        let edit = &edit;
        stream::iter(edits)
            .map(|(key, edits)| async move {
                let old = self.lookup(root.as_ref(), key).await?;
                let node = edit(old.as_deref(), edits);
                let hash = if node.is_empty() {
                    None
                } else {
                    Some(self.write_node(&node).await?)
                };
                Ok((key, hash))
            })
            .buffer_unordered(self.parallel)
            .try_collect()
            .await
    }

    /// Hashes the data and writes into the store.
    /// Returns the object hash.
    async fn write_object(
        &self,
        data: Vec<u8>,
        is_data: bool,
    ) -> MerkleResult<SizedHash, B::Error> {
        let hash = SizedHash::new(ContentHash::of(&data), data.len());
        trace!(hash = ?hash, "write");
        self.backend.write(hash.hash(), data, is_data).await?;
        Ok(hash)
    }

    async fn write_node(&self, node: &Node) -> MerkleResult<SizedHash, B::Error> {
        trace!(?node, "write_node");
        let data = node.serialize();
        self.write_object(data, false).await
    }
}

impl<B: TreeReader> MerkleStore<B> {
    /// Lists every name in the tree with its data hash.
    pub async fn enumerate(&self) -> MerkleResult<Vec<(String, SizedHash)>, B::Error> {
        let mut names = Vec::new();
        self.walk(|_, node| {
            if let Node::Leaf(leaf) = node {
                names.extend(leaf.iter().map(|(name, hash)| (name.clone(), *hash)));
            }
        })
        .await?;
        Ok(names)
    }

    /// Calls `visit` with every node reachable from the root, loading nodes
    /// concurrently.
    async fn walk(&self, mut visit: impl FnMut(&SizedHash, &Node)) -> MerkleResult<(), B::Error> {
        let Some(root) = self.root() else {
            return Ok(());
        };
        let load = async |hash: SizedHash| {
            let node = self.load_node(&hash).await?;
            MerkleResult::<_, B::Error>::Ok((hash, node))
        };
        let mut pending = FuturesUnordered::new();
        pending.push(load(root));
        while let Some((hash, node)) = pending.try_next().await? {
            visit(&hash, &node);
            if let Node::Index(index) = &*node {
                for child in index.iter() {
                    pending.push(load(*child));
                }
            }
        }
        Ok(())
    }
}

impl<B: TreeEnumerator> MerkleStore<B> {
    /// Deletes every object not reachable from the current root or from a
    /// pending (uncommitted) write.
    ///
    /// Takes `&mut self` so no `put_object` or `commit` on this store can run
    /// at the same time. It does not protect against other processes writing
    /// to the same backend.
    pub async fn gc(&mut self) -> MerkleResult<(), B::Error> {
        debug!("starting GC");

        let all = self.backend.enumerate_all().await?;
        debug!(all = all.len(), "all objects enumerated");

        let mut live = HashSet::new();
        self.walk(|hash, node| {
            live.insert((*hash.hash(), false));
            if let Node::Leaf(leaf) = node {
                live.extend(leaf.iter().map(|(_, hash)| (*hash.hash(), true)));
            }
        })
        .await?;
        for (_, hash) in self.pending_writes.get_mut().unwrap().iter() {
            if let Some(hash) = hash {
                live.insert((*hash.hash(), true));
            }
        }
        debug!(live = live.len(), "live objects enumerated");

        let dead: Vec<_> = all.difference(&live).cloned().collect();
        debug!(dead = dead.len(), "GC completed");

        for (entry, is_data) in &dead {
            trace!(hash = ?entry, is_data, "deleted");
            self.backend.delete(entry, *is_data).await?
        }
        debug!("GC delete completed");

        Ok(())
    }
}
