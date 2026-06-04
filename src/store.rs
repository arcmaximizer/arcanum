// Content-addressed store
use anyhow::Result;
use bytes::Bytes;
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::path::Path;
use tar::Archive;
use tokio::sync::{mpsc, oneshot};

pub type HashKey = [u8; 32];

#[derive(Debug)]
pub enum StoreMsg {
    ResolveName {
        name: String,
        resp: tokio::sync::oneshot::Sender<Option<HashKey>>,
    },
    SetName {
        name: String,
        key: HashKey,
    },
    GetPackage {
        key: HashKey,
        resp: tokio::sync::oneshot::Sender<Option<Bytes>>,
    },
    AddPackage {
        value: Bytes,
        resp: tokio::sync::oneshot::Sender<Result<HashKey>>,
    },
    GetAsset {
        key: HashKey,
        asset: String,
        resp: tokio::sync::oneshot::Sender<Option<Bytes>>,
    },
    GetAssetByName {
        name: String,
        asset: String,
        resp: tokio::sync::oneshot::Sender<Option<Bytes>>,
    },
}

#[derive(Clone)]
pub struct StoreHandle {
    sender: mpsc::UnboundedSender<StoreMsg>,
}

impl StoreHandle {
    pub fn new(store: Box<dyn PackageStore + Send>) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_store(receiver, store));
        Self { sender }
    }

    pub fn from_sender(sender: mpsc::UnboundedSender<StoreMsg>) -> Self {
        Self { sender }
    }

    pub async fn resolve_name(&self, name: String) -> Option<HashKey> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StoreMsg::ResolveName {
                name,
                resp: resp_tx,
            })
            .expect("Store task has been killed");
        resp_rx.await.expect("Store task has been killed")
    }

    pub fn set_name(&self, name: String, key: HashKey) {
        self.sender
            .send(StoreMsg::SetName { name, key })
            .expect("Store task has been killed");
    }

    pub async fn get_package(&self, key: HashKey) -> Option<Bytes> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StoreMsg::GetPackage { key, resp: resp_tx })
            .expect("Store task has been killed");
        resp_rx.await.expect("Store task has been killed")
    }

    pub async fn add_package(&self, value: Bytes) -> Result<HashKey> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StoreMsg::AddPackage {
                value,
                resp: resp_tx,
            })
            .expect("Store task has been killed");
        resp_rx.await.expect("Store task has been killed")
    }

    pub async fn get_asset(&self, key: HashKey, asset: String) -> Option<Bytes> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StoreMsg::GetAsset {
                key,
                asset,
                resp: resp_tx,
            })
            .expect("Store task has been killed");
        resp_rx.await.expect("Store task has been killed")
    }

    pub async fn get_asset_by_name(&self, name: String, asset: String) -> Option<Bytes> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StoreMsg::GetAssetByName {
                name,
                asset,
                resp: resp_tx,
            })
            .expect("Store task has been killed");
        resp_rx.await.expect("Store task has been killed")
    }
}

pub async fn run_store(
    mut rx: mpsc::UnboundedReceiver<StoreMsg>,
    mut store: Box<dyn PackageStore + Send>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            StoreMsg::ResolveName { name, resp } => {
                let key = store.resolve_name(&name);
                let _ = resp.send(key);
            }
            StoreMsg::SetName { name, key } => {
                store.set_name(&name, key);
            }
            StoreMsg::GetPackage { key, resp } => {
                let pkg = store.get_package(&key);
                let _ = resp.send(pkg);
            }
            StoreMsg::AddPackage { value, resp } => {
                let key = store.add_package(value);
                let _ = resp.send(key);
            }
            StoreMsg::GetAsset { key, asset, resp } => {
                let asset_data = store.get_asset(&key, &asset);
                let _ = resp.send(asset_data);
            }
            StoreMsg::GetAssetByName { name, asset, resp } => {
                let result = store
                    .resolve_name(&name)
                    .and_then(|key| store.get_asset(&key, &asset));
                let _ = resp.send(result);
            }
        }
    }
}

pub trait PackageStore {
    fn resolve_name(&self, name: &str) -> Option<HashKey>;
    fn set_name(&mut self, name: &str, key: HashKey);
    fn get_package(&self, key: &HashKey) -> Option<Bytes>;
    fn add_package(&mut self, value: Bytes) -> Result<HashKey>;
    fn get_asset(&self, key: &HashKey, asset: &str) -> Option<Bytes>;
}

#[derive(Default)]
pub struct InMemoryPackageStore {
    names: HashMap<String, HashKey>,
    packages: HashMap<HashKey, Bytes>,
    cache: HashMap<(HashKey, String), Bytes>,
}

impl InMemoryPackageStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PackageStore for InMemoryPackageStore {
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

        if format.is_none() {
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

pub struct SqlitePackageStore {
    conn: rusqlite::Connection,
    cache: HashMap<(HashKey, String), Bytes>,
}

impl SqlitePackageStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = rusqlite::Connection::open(path.as_ref())?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS packages (
                hash BLOB PRIMARY KEY,
                data BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS names (
                name TEXT PRIMARY KEY,
                hash BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS assets (
                hash BLOB NOT NULL,
                path TEXT NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (hash, path)
            );",
        )?;
        Ok(Self {
            conn,
            cache: HashMap::new(),
        })
    }
}

impl PackageStore for SqlitePackageStore {
    fn resolve_name(&self, name: &str) -> Option<HashKey> {
        self.conn
            .query_row(
                "SELECT hash FROM names WHERE name = ?1",
                rusqlite::params![name],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .ok()
            .map(|v| {
                let mut key = [0u8; 32];
                key.copy_from_slice(&v);
                key
            })
    }

    fn set_name(&mut self, name: &str, key: HashKey) {
        let _ = self.conn.execute(
            "INSERT OR REPLACE INTO names (name, hash) VALUES (?1, ?2)",
            rusqlite::params![name, key.to_vec()],
        );
    }

    fn get_package(&self, key: &HashKey) -> Option<Bytes> {
        self.conn
            .query_row(
                "SELECT data FROM packages WHERE hash = ?1",
                rusqlite::params![key.to_vec()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .ok()
            .map(Bytes::from)
    }

    fn add_package(&mut self, value: Bytes) -> Result<HashKey> {
        let key: HashKey = Sha256::digest(&value).into();
        let format = detect_tar(&value);

        if format.is_none() {
            anyhow::bail!("Invalid tarball")
        }

        // Check if already stored
        if self
            .conn
            .query_row(
                "SELECT 1 FROM packages WHERE hash = ?1",
                rusqlite::params![key.to_vec()],
                |_| Ok(()),
            )
            .is_ok()
        {
            anyhow::bail!("Package already exists")
        }

        self.conn.execute(
            "INSERT INTO packages (hash, data) VALUES (?1, ?2)",
            rusqlite::params![key.to_vec(), value.to_vec()],
        )?;

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
            self.conn.execute(
                "INSERT OR REPLACE INTO assets (hash, path, data) VALUES (?1, ?2, ?3)",
                rusqlite::params![key.to_vec(), path_str, contents],
            )?;
            self.cache
                .insert((key, path_str), contents.into());
        }

        Ok(key)
    }

    fn get_asset(&self, key: &HashKey, asset: &str) -> Option<Bytes> {
        if let Some(cached) = self.cache.get(&(*key, asset.to_string())) {
            return Some(cached.clone());
        }
        self.conn
            .query_row(
                "SELECT data FROM assets WHERE hash = ?1 AND path = ?2",
                rusqlite::params![key.to_vec(), asset],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .ok()
            .map(Bytes::from)
    }
}

/// Scan a directory for .tar.gz files and add them as named packages.
/// The filename (without extension) becomes the package name in the store,
/// named as `^local/<filename>`.
pub async fn load_packages_from_dir(
    store: &StoreHandle,
    dir: impl AsRef<Path>,
) -> Result<usize> {
    let dir = dir.as_ref();
    if !dir.is_dir() {
        return Ok(0);
    }
    let mut count = 0;
    let mut entries: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "tar" || ext == "gz")
                .unwrap_or(false)
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in &entries {
        let path = entry.path();
        let data = Bytes::from(std::fs::read(&path)?);
        match store.add_package(data).await {
            Ok(key) => {
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown");
                let name = format!("^local/{}", stem);
                store.set_name(name, key);
                count += 1;
            }
            Err(_) => {
                // Skip duplicates and invalid tarballs
            }
        }
    }
    Ok(count)
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
