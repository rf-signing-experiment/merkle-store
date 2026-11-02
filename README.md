# merkletree

A content-addressed Merkle tree that maps file names to blobs, stored through a
pluggable async backend. Runtime-agnostic: it depends only on `futures`.

## Example

```rust
use merkletree::{RwMerkleStore, TreeParameters, memstore::MemoryStore};

let store = RwMerkleStore::new(
    MemoryStore::new(),
    8,                                         // max concurrent writes per tree level
    TreeParameters { depth: 2, breadth: 8 },   // 2 index levels of 256 slots each
);

store.put_object("hello.txt", b"hi".to_vec()).await?;
store.commit().await?;                         // builds the new tree, swaps the root

let root = store.root().await;                 // persist this yourself
assert_eq!(store.get_file("hello.txt").await?, Some(b"hi".to_vec()));
```

To reopen a tree, construct a store over the same backend and call
`set_root(saved_root)`. The library never persists the root hash.

## How it works

- **Blobs** are stored under their `ContentHash` (a size-class byte followed by
  the SHA-256 of the content). Every read is verified against its hash.
- **Names** are hashed to 32 bits. Each index level uses `breadth` of those
  bits (lowest bits first) to pick one of `2^breadth` children. Below the last
  index level, a leaf node holds a sorted list of `(name, hash)` pairs.
- **Writes** are two-phase: `put_object` uploads the blob right away and queues
  the name; `commit` rewrites only the affected leaves and index nodes
  (copy-on-write) and then swaps the root.
- **GC** (`RwMerkleStore::gc`) deletes every object not reachable from the
  current root or from an uncommitted `put_object`, including objects that
  only older roots reference. It takes `&mut self`, so it can't overlap with
  writes on the same store. Nothing stops another process from writing to the
  same backend during GC.

## Backends

| Backend | Description |
|---|---|
| `memstore::MemoryStore` | In-memory hash maps, for tests. |
| `fsstore::FsStore` | Files under `tree/<hex>` and `data/<xx>/<rest>`, written atomically. |
| `fscache::FsCache<B>` | A local directory cache in front of another backend. Bad cache entries are detected and fetched again. |

Implement `TreeReader` for read-only access, `TreeWriter` for writes, and
`TreeEnumerator` to enable GC.

## Status

Early and experimental. Known gaps:

- `delete(name)` is a no-op.
- The tree parameters aren't stored in the tree, so a tree must always be
  written with the same `TreeParameters` it was created with.
- The filesystem backends use blocking `std::fs` calls inside async fns.

## Development

```sh
cargo build
cargo test
```
