use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Mutex,
};

use crate::{TreeEnumerator, TreeReader, TreeWriter, types::ContentHash};

/// Keeps objects in memory, keyed by `(hash, is_data)`.
#[derive(Default)]
pub struct MemoryStore {
    objects: Mutex<HashMap<(ContentHash, bool), Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TreeReader for MemoryStore {
    type Error = Infallible;

    async fn read(&self, hash: &ContentHash, is_data: bool) -> Result<Option<Vec<u8>>, Infallible> {
        Ok(self.objects.lock().unwrap().get(&(*hash, is_data)).cloned())
    }
}

impl TreeWriter for MemoryStore {
    async fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_data: bool,
    ) -> Result<(), Infallible> {
        self.objects.lock().unwrap().insert((*hash, is_data), data);
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash, is_data: bool) -> Result<(), Infallible> {
        self.objects.lock().unwrap().remove(&(*hash, is_data));
        Ok(())
    }
}

impl TreeEnumerator for MemoryStore {
    async fn enumerate_all(&self) -> Result<HashSet<(ContentHash, bool)>, Infallible> {
        Ok(self.objects.lock().unwrap().keys().copied().collect())
    }
}
