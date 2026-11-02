use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use tracing::error;

use crate::{TreeEnumerator, TreeReader, TreeWriter, types::ContentHash};

pub struct FsCache<B> {
    inner: B,
    root: PathBuf,
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

const TREE: &'static str = "tree";
const DATA: &'static str = "data";

impl<B> FsCache<B> {
    pub fn new(root: impl AsRef<Path>, backend: B) -> IoResult<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(root.join(TREE))?;
        std::fs::create_dir_all(root.join(DATA))?;
        Ok(Self {
            root,
            inner: backend,
        })
    }

    fn object_path(&self, hash: &ContentHash, is_data: bool) -> PathBuf {
        let hash = hash.to_string();
        if is_data {
            self.root.join(DATA).join(&hash[0..2]).join(&hash[2..])
        } else {
            self.root.join(TREE).join(hash)
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
        let path = self.object_path(&hash, is_data);
        match std::fs::read(&path) {
            Ok(data) => return Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                error!("{e}");
            }
        }
        let result = self.inner.read(hash, is_data).await;
        if let Ok(Some(data)) = &result {
            let _ = std::fs::create_dir_all(path.parent().unwrap());
            if let Err(e) = std::fs::write(&path, data) {
                error!("{e}");
            }
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
        let path = self.object_path(&hash, is_data);
        let _ = std::fs::create_dir_all(path.parent().unwrap());
        if !path.exists() {
            if !is_data && let Err(e) = std::fs::write(&path, &data) {
                error!("{e}");
            }
            self.inner.write(hash, data, is_data).await?;
        }
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash, is_data: bool) -> Result<(), Self::Error> {
        let path = self.object_path(&hash, is_data);
        if let Err(e) = std::fs::remove_file(&path) {
            error!("{e}");
        }
        self.inner.delete(hash, is_data).await
    }
}

impl<B: TreeEnumerator> TreeEnumerator for FsCache<B> {
    async fn enumerate_all(&self) -> Result<HashSet<(ContentHash, bool)>, Self::Error> {
        self.inner.enumerate_all().await
    }
}
