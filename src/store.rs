// Content-addressed store
use anyhow::Result;
use bytes::Bytes;
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{Cursor, Read};
use tar::Archive;

pub type HashKey = [u8; 32];

pub trait PackageStore {
    fn resolve_name(&self, name: &str) -> Option<HashKey>;
    fn set_name(&mut self, name: &str, key: HashKey);
    fn get_package(&self, key: &HashKey) -> Option<Bytes>;
    fn add_package(&mut self, value: Bytes) -> Result<HashKey>;
    fn get_asset(&self, key: &HashKey, asset: &str) -> Option<Bytes>;
}

pub struct MemoryPackageStore {
    names: HashMap<String, HashKey>,
    packages: HashMap<HashKey, Bytes>,
    cache: HashMap<(HashKey, String), Bytes>,
}

impl PackageStore for MemoryPackageStore {
    fn resolve_name(&self, name: &str) -> Option<HashKey> {
        self.names.get(name).copied()
    }
    fn set_name(&mut self, name: &str, key: HashKey) {
        self.names.insert(name.to_string(), key);
    }
    fn get_package(&self, key: &HashKey) -> Option<Bytes> {
        self.packages.get(key).cloned()
    }
    fn add_package(&mut self, value: Bytes) -> Result<HashKey> {
        let key: HashKey = Sha256::digest(&value).into();
        let format = detect_tar(&value);

        if format == None {
            anyhow::bail!("Invalid tarball")
        }
        if self.packages.contains_key(&key) {
            anyhow::bail!("Package already exists")
        }

        self.packages.insert(key, value.clone());

        let format = format.unwrap();
        let reader: Box<dyn Read> = if format == ".tar.gz" {
            Box::new(GzDecoder::new(&value[..]))
        } else {
            Box::new(Cursor::new(&value[..]))
        };
        let mut archive = Archive::new(reader);
        for entry in archive.entries().map_err(|e| anyhow::anyhow!("{}", e))? {
            let mut entry = entry.map_err(|e| anyhow::anyhow!("{}", e))?;
            let path = entry.path().map_err(|e| anyhow::anyhow!("{}", e))?;
            let path_str = path.to_string_lossy().into_owned();
            let mut contents = Vec::new();
            entry
                .read_to_end(&mut contents)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            self.cache.insert((key, path_str), contents.into());
        }

        Ok(key)
    }
    fn get_asset(&self, key: &HashKey, asset: &str) -> Option<Bytes> {
        self.cache.get(&(*key, asset.to_string())).cloned()
    }
}

fn detect_tar(data: &Bytes) -> Option<&'static str> {
    if data.len() < 2 {
        return None;
    }
    if data[0] == 0x1f && data[1] == 0x8b {
        return Some(".tar.gz");
    }
    if data.len() >= 300 && &data[257..262] == b"ustar" {
        return Some(".tar");
    }
    None
}
