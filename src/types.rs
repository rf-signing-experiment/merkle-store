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
pub struct IndexNode(Vec<ContentHash>);

impl IndexNode {
    pub fn new(nibble_width: u8) -> Self {
        assert!(nibble_width > 0);
        Self(vec![
            ContentHash([0; ContentHash::SIZE]);
            ContentHash::count(nibble_width)
        ])
    }

    pub fn len_bits(&self) -> u8 {
        let breadth = self.0.len().ilog2() as u8;
        debug_assert!(1 << (breadth as usize) == self.0.len());
        breadth
    }

    pub fn get(&self, key: NameHash) -> Option<&ContentHash> {
        let v = self.0.get(key.0 as usize).unwrap();
        if v.0 == [0; ContentHash::SIZE] {
            None
        } else {
            Some(v)
        }
    }

    pub fn insert(&mut self, key: NameHash, value: ContentHash) {
        self.0[key.0 as usize] = value;
    }

    /// Empties the slot for `key`.
    pub fn remove(&mut self, key: NameHash) {
        self.0[key.0 as usize] = ContentHash([0; ContentHash::SIZE]);
    }

    /// True if every slot is empty.
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn iter(&self) -> impl Iterator<Item = &ContentHash> {
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

#[derive(Clone, BorshDeserialize, BorshSerialize)]
pub struct LeafNode(Vec<(String, ContentHash)>);

impl LeafNode {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn get(&self, key: &str) -> Option<&ContentHash> {
        self.0
            .binary_search_by_key(&key, |v| &v.0)
            .ok()
            .map(|v| &self.0[v].1)
    }

    /// Inserts `key`, replacing its value if it is already present.
    pub fn insert(&mut self, key: String, value: ContentHash) {
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

    pub fn iter(&self) -> impl Iterator<Item = (&String, &ContentHash)> {
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

#[derive(Clone, Copy, PartialEq, Eq, BorshDeserialize, BorshSerialize)]
pub struct ContentHash([u8; ContentHash::SIZE]);

impl std::hash::Hash for ContentHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(&self.0[ContentHash::SIZE - size_of::<u64>()..]);
    }
}

impl Debug for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = [0u8; ContentHash::SIZE * 2];
        hex::encode_to_slice(&self.0, &mut output).unwrap();
        f.write_str(std::str::from_utf8(&output).unwrap())
    }
}

impl From<[u8; ContentHash::SIZE]> for ContentHash {
    fn from(value: [u8; ContentHash::SIZE]) -> Self {
        ContentHash(value)
    }
}

impl TryFrom<&str> for ContentHash {
    type Error = FromHexError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut data = [0u8; ContentHash::SIZE];
        hex::decode_to_slice(value.as_bytes(), &mut data)?;
        Ok(ContentHash(data))
    }
}

impl ContentHash {
    const SIZE: usize = 33;

    pub fn size_limit(&self) -> usize {
        1 << self.0[0].min(usize::BITS as u8)
    }

    pub fn hash(&self) -> &[u8; 32] {
        self.0[1..].try_into().unwrap()
    }

    pub fn count(nibble_width: u8) -> usize {
        1 << nibble_width
    }

    pub fn is_null(&self) -> bool {
        self.0 == [0u8; Self::SIZE]
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
