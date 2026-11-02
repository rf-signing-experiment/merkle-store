use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Mutex,
};

use crate::{TreeEnumerator, TreeReader, TreeWriter, types::ContentHash};

pub struct MemoryStore {
    data: Mutex<HashMap<ContentHash, Vec<u8>>>,
    tree: Mutex<HashMap<ContentHash, Vec<u8>>>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            tree: Mutex::new(HashMap::new()),
        }
    }
}

impl TreeReader for MemoryStore {
    type Error = Infallible;

    async fn read(
        &self,
        hash: &ContentHash,
        is_data: bool,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        if is_data {
            Ok(self.data.lock().unwrap().get(hash).cloned())
        } else {
            Ok(self.tree.lock().unwrap().get(hash).cloned())
        }
    }
}

impl TreeWriter for MemoryStore {
    async fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_data: bool,
    ) -> Result<(), Self::Error> {
        if is_data {
            self.data.lock().unwrap().insert(*hash, data);
        } else {
            self.tree.lock().unwrap().insert(*hash, data);
        }
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash, is_data: bool) -> Result<(), Self::Error> {
        if is_data {
            self.data.lock().unwrap().remove(hash);
        } else {
            self.tree.lock().unwrap().remove(hash);
        }
        Ok(())
    }
}

impl TreeEnumerator for MemoryStore {
    async fn enumerate_all(&self) -> Result<HashSet<(ContentHash, bool)>, Self::Error> {
        let mut result = HashSet::new();
        for k in self.tree.lock().unwrap().keys() {
            result.insert((*k, false));
        }
        for k in self.data.lock().unwrap().keys() {
            result.insert((*k, true));
        }
        Ok(result)
    }
}
