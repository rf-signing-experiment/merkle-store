use std::fmt::{Debug, Display};

use borsh::{BorshDeserialize, BorshSerialize};
use hex::FromHexError;
use sha2::{Digest as _, Sha256};

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
pub enum Node {
    Index(IndexNode),
    Leaf(LeafNode),
}

impl Node {
    pub fn serialize(&self) -> Vec<u8> {
        borsh::to_vec(&self).unwrap()
    }

    pub fn deserialize(data: Vec<u8>) -> Self {
        borsh::from_slice(&data).unwrap()
    }

    /// True for a leaf with no names or an index node with no children.
    pub fn is_empty(&self) -> bool {
        match self {
            Node::Index(index) => index.is_empty(),
            Node::Leaf(leaf) => leaf.is_empty(),
        }
    }

    pub fn as_leaf(&self) -> Option<&LeafNode> {
        match self {
            Node::Leaf(leaf_node) => Some(leaf_node),
            _ => None,
        }
    }

    pub fn as_index(&self) -> Option<&IndexNode> {
        match self {
            Node::Index(index_node) => Some(index_node),
            _ => None,
        }
    }
}

#[derive(Clone, BorshDeserialize, BorshSerialize)]
pub struct IndexNode(Vec<SizedHash>);

impl IndexNode {
    pub fn new(nibble_width: u8) -> Self {
        assert!(nibble_width > 0);
        Self(vec![SizedHash::NULL; ContentHash::count(nibble_width)])
    }

    pub fn len_bits(&self) -> u8 {
        let breadth = self.0.len().ilog2() as u8;
        debug_assert!(1 << (breadth as usize) == self.0.len());
        breadth
    }

    pub fn get(&self, key: NameHash) -> Option<&SizedHash> {
        let v = &self.0[key.0 as usize];
        if v.is_null() { None } else { Some(v) }
    }

    pub fn insert(&mut self, key: NameHash, value: SizedHash) {
        self.0[key.0 as usize] = value;
    }

    /// Empties the slot for `key`.
    pub fn remove(&mut self, key: NameHash) {
        self.0[key.0 as usize] = SizedHash::NULL;
    }

    /// True if every slot is empty.
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn iter(&self) -> impl Iterator<Item = &SizedHash> {
        self.0.iter().filter(|node| !node.is_null())
    }
}

impl Debug for IndexNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (index, hash) in self.0.iter().enumerate() {
            if !hash.is_null() {
                m.entry(&NameHash(index as u32, self.len_bits()), hash);
            }
        }
        m.finish()
    }
}

#[derive(Clone, Default, BorshDeserialize, BorshSerialize)]
pub struct LeafNode(Vec<(String, SizedHash)>);

impl LeafNode {
    pub fn get(&self, key: &str) -> Option<&SizedHash> {
        self.0
            .binary_search_by_key(&key, |v| &v.0)
            .ok()
            .map(|v| &self.0[v].1)
    }

    /// Inserts `key`, replacing its value if it is already present.
    pub fn insert(&mut self, key: String, value: SizedHash) {
        match self.0.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(index) => self.0[index].1 = value,
            Err(index) => self.0.insert(index, (key, value)),
        }
    }

    /// Removes `key` if present.
    pub fn remove(&mut self, key: &str) {
        if let Ok(index) = self.0.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
            self.0.remove(index);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &SizedHash)> {
        self.0.iter().map(|(k, v)| (k, v))
    }
}

impl Debug for LeafNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (index, hash) in self.0.iter() {
            m.entry(index, hash);
        }
        m.finish()
    }
}

/// The SHA-256 of an object's content. Backends name objects by it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, BorshDeserialize, BorshSerialize)]
pub struct ContentHash([u8; 32]);

impl ContentHash {
    pub(crate) const NULL: Self = Self([0; 32]);

    /// The hash of `data`, as used for every stored object.
    pub fn of(data: &[u8]) -> Self {
        Self(Sha256::digest(data).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn count(nibble_width: u8) -> usize {
        1 << nibble_width
    }

    pub fn is_null(&self) -> bool {
        *self == Self::NULL
    }
}

impl From<[u8; 32]> for ContentHash {
    fn from(sha256: [u8; 32]) -> Self {
        Self(sha256)
    }
}

/// Hex of the SHA-256.
impl Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl Debug for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

/// Parses the hex form produced by `Display`.
impl std::str::FromStr for ContentHash {
    type Err = FromHexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(value, &mut bytes)?;
        Ok(Self(bytes))
    }
}

/// A `ContentHash` plus a size class that bounds the object's length. Tree
/// nodes refer to their children this way, so every read knows how large
/// the object can be. The all-zero value (`NULL`) marks an empty slot in an
/// index node.
///
/// Borsh serializes it as 33 bytes in field order (size class, then
/// SHA-256), so don't reorder the fields: that would change the node format.
#[derive(Clone, Copy, PartialEq, Eq, Hash, BorshDeserialize, BorshSerialize)]
pub struct SizedHash {
    /// 0 for empty content, otherwise `ilog2(len) + 1`, so `len < 2^size_class`.
    size_class: u8,
    hash: ContentHash,
}

impl SizedHash {
    pub(crate) const NULL: Self = Self {
        size_class: 0,
        hash: ContentHash::NULL,
    };

    /// Pairs `hash` with the size class of content that is `len` bytes long.
    pub(crate) fn new(hash: ContentHash, len: usize) -> Self {
        Self {
            size_class: len.checked_ilog2().map_or(0, |l| l as u8 + 1),
            hash,
        }
    }

    pub fn hash(&self) -> &ContentHash {
        &self.hash
    }

    /// The most bytes the object can have: `2^size_class - 1`.
    pub fn size_limit(&self) -> usize {
        let class = u32::from(self.size_class).min(usize::BITS);
        // Shifting by the full width (class 0) gives `None`: the limit is 0.
        usize::MAX.checked_shr(usize::BITS - class).unwrap_or(0)
    }

    fn is_null(&self) -> bool {
        *self == Self::NULL
    }
}

/// Hex of the 33 serialized bytes.
impl Display for SizedHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:02x}{}", self.size_class, self.hash)
    }
}

impl Debug for SizedHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

/// Parses the hex form produced by `Display`.
impl std::str::FromStr for SizedHash {
    type Err = FromHexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0u8; 33];
        hex::decode_to_slice(value, &mut bytes)?;
        Ok(Self {
            size_class: bytes[0],
            hash: ContentHash(bytes[1..].try_into().unwrap()),
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NameHash(u32, u8);

impl NameHash {
    const MAX: u8 = 32;

    pub fn new(value: &str, len_in_bits: u8) -> NameHash {
        assert!(len_in_bits <= Self::MAX);
        let bytes = Sha256::digest(value.as_bytes());
        NameHash(u32::from_le_bytes(bytes[..4].try_into().unwrap()), 32)
            .split(len_in_bits)
            .1
    }

    /// Split the key, indexed from the right (0 is the rightmost).
    pub fn split(self, at: u8) -> (NameHash, NameHash) {
        debug_assert!(at <= self.1);
        let x = self.0;
        let left = x.checked_shr(at as u32).unwrap_or(0);
        let right = x & ((1u64 << at) - 1) as u32;
        (NameHash(left, self.1 - at), NameHash(right, at))
    }

    pub fn is_empty(self) -> bool {
        self.1 == 0
    }
}

impl Debug for NameHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:0w$b}({})", self.0, self.1, w = self.1 as usize,)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_hash_serializes_as_32_bytes() {
        let hash = ContentHash([7; 32]);
        let bytes = borsh::to_vec(&hash).unwrap();
        assert_eq!(bytes, [7; 32]);
        assert_eq!(borsh::from_slice::<ContentHash>(&bytes).unwrap(), hash);
        assert_eq!(hash.to_string().parse::<ContentHash>().unwrap(), hash);
    }

    #[test]
    fn sized_hash_serializes_as_33_bytes() {
        let hash = SizedHash::new(ContentHash([7; 32]), 1000);
        let bytes = borsh::to_vec(&hash).unwrap();
        // ilog2(1000) + 1 = 10, then the SHA-256 with no length prefix.
        assert_eq!(bytes, [&[10u8][..], &[7; 32]].concat());
        assert_eq!(borsh::from_slice::<SizedHash>(&bytes).unwrap(), hash);
        assert_eq!(hash.to_string().parse::<SizedHash>().unwrap(), hash);
        assert_eq!(hash.to_string(), format!("0a{}", hash.hash()));
    }

    #[test]
    fn size_limit_is_tightest_bound() {
        let limit = |len| SizedHash::new(ContentHash([7; 32]), len).size_limit();
        assert_eq!(limit(0), 0);
        assert_eq!(limit(1), 1);
        assert_eq!(limit(2), 3);
        assert_eq!(limit(3), 3);
        assert_eq!(limit(4), 7);
        assert_eq!(limit(1000), 1023);
        assert_eq!(limit(1024), 2047);
        assert_eq!(limit(usize::MAX), usize::MAX);
        // A size class too large for any length doesn't overflow.
        let huge = SizedHash {
            size_class: 255,
            hash: ContentHash([7; 32]),
        };
        assert_eq!(huge.size_limit(), usize::MAX);
    }

    #[test]
    fn basic() {
        let (l, r) = NameHash(0b1010_1100_1111_0001_0011_0101_1001_0110, 32).split(16);
        assert_eq!(l.0, 0b1010_1100_1111_0001);
        assert_eq!(r.0, 0b0011_0101_1001_0110);

        let (l, r) = NameHash(0b1010_1100_1111_0001_0011_0101_1001_0110, 32).split(17);
        assert_eq!(l.0, 0b1010_1100_1111_000);
        assert_eq!(r.0, 0b10011_0101_1001_0110);
    }

    #[test]
    fn limits() {
        let x = 0b1010_1100_1111_0001_0011_0101_1001_0110;
        let (l, r) = NameHash(x, 32).split(0);
        assert_eq!(l.0, x);
        assert_eq!(r.0, 0);
    }

    fn mk(data: u32) -> NameHash {
        NameHash(data, 32)
    }

    /// Takes the `index`-th `width`-bit window, counting from the most
    /// significant bit (the semantics of the old `subkey` API), using `split`.
    fn window(k: NameHash, index: u8, width: u8) -> NameHash {
        let (top, _) = k.split(32 - (index + 1) * width);
        top.split(width).1
    }

    #[test]
    fn full_range_identity() {
        let d = 0b1010_1100_1111_0001_0011_0101_1001_0110;
        let v = window(mk(d), 0, 31);
        assert_eq!(v.0, d >> 1);
    }

    #[test]
    fn single_bit_basic() {
        let k = mk(0b1000_0000_0000_0000_0000_0000_0000_0000);
        assert_eq!(window(k, 0, 1).0, 1);

        let k = mk(0b0100_0000_0000_0000_0000_0000_0000_0000);
        assert_eq!(window(k, 1, 1).0, 1);
    }

    #[test]
    fn simple_4bit_window() {
        let k = mk(0b1011_0000_0000_0000_0000_0000_0000_0000);

        // first 4 bits = 1011
        assert_eq!(window(k, 0, 4).0, 0b1011);
    }

    #[test]
    fn middle_slice() {
        let k = mk(0b1111_0000_1010_1100_0000_0000_0000_0000);

        // second 4-bit window
        assert_eq!(window(k, 1, 4).0, 0b0000);
        // fifth 4-bit window (bits 16..20)
        assert_eq!(window(k, 4, 4).0, 0b0000);
        // third and fourth 4-bit windows
        assert_eq!(window(k, 2, 4).0, 0b1010);
        assert_eq!(window(k, 3, 4).0, 0b1100);
    }

    #[test]
    fn cross_byte_boundary() {
        let k = mk(0b0000_1111_1111_0000_0000_0000_0000_0000);

        // second byte
        let v = window(k, 1, 8);
        assert_eq!(v.0, 0b1111_0000);
    }

    #[test]
    fn cross_byte_boundary_large() {
        let k = mk(0b0000_0000_0000_1101_0111_1011_1000_0000);

        // bits 13..26, spanning several bytes
        let v = window(k, 1, 13);
        assert_eq!(v.0, 0b101_0111_1011_10);
    }

    #[test]
    fn max_allowed_slice_31_bits() {
        let v = window(mk(0xFFFF_FFFF), 0, 31);
        assert_eq!(v.0, 0x7FFF_FFFF);
    }

    #[test]
    fn deterministic_repeatable() {
        let k1 = mk(0x1234_5678);

        assert_eq!(window(k1, 1, 16).0, 0x5678);
        assert_eq!(window(k1, 1, 16).0, window(k1, 1, 16).0);
    }
}
