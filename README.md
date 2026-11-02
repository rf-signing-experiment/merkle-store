# merkle-store

A content-addressed Merkle tree that maps file names to blobs, stored through a
pluggable async backend. Runtime-agnostic: it depends only on `futures`.

## Example

```rust
use merkle_store::{MerkleStore, TreeParameters, memstore::MemoryStore};

let store = MerkleStore::new(
    MemoryStore::new(),
    8,                                         // max concurrent writes per tree level
    TreeParameters { depth: 2, breadth: 8 },   // 2 index levels of 256 slots each
);

store.put_object("hello.txt", b"hi".to_vec()).await?;
store.commit().await?;                         // builds the new tree, swaps the root

let root = store.root();                       // persist this yourself
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
  the name, and `delete` queues a removal; `commit` rewrites only the affected
  leaves and index nodes (copy-on-write) and then swaps the root. Nodes left
  empty by deletes are dropped, so the tree only depends on the names in it.
  A deleted file's data stays in the backend until GC.
- **GC** (`MerkleStore::gc`) deletes every object not reachable from the
  current root.

## Backends

| Backend | Description |
|---|---|
| `memstore::MemoryStore` | In-memory hash maps, for tests. |
| `fsstore::FsStore` | Files named by their SHA-256: `tree/<sha256>` and `data/<xx>/<rest>`, written atomically. |
| `cache::Cache<L, R>` | Backend `L` as a local cache in front of backend `R`. Bad cache entries are detected and fetched again. `cache::FsCache<R>` uses an `FsStore` directory as the cache. |

Implement `TreeReader` for read-only access, `TreeWriter` for writes, and
`TreeEnumerator` to enable GC. `MerkleStore<B>` offers whichever methods
the backend supports.

## Development

```sh
cargo build
cargo test
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
