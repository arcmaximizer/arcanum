// Content-addressed store
use anyhow::Result;
use bytes::Bytes;
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
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
    ListNames {
        resp: tokio::sync::oneshot::Sender<Vec<String>>,
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

    pub async fn list_names(&self) -> Vec<String> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StoreMsg::ListNames { resp: resp_tx })
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
            StoreMsg::ListNames { resp } => {
                let names = store.list_names();
                let _ = resp.send(names);
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
    fn list_names(&self) -> Vec<String>;
}

pub struct FileSystemPackageStore {
    dir: PathBuf,
    names: HashMap<String, HashKey>,
    packages: HashMap<HashKey, Bytes>,
    cache: HashMap<(HashKey, String), Bytes>,
}

impl FileSystemPackageStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let mut store = Self {
            dir,
            names: HashMap::new(),
            packages: HashMap::new(),
            cache: HashMap::new(),
        };

        store.load_names()?;
        store.load_packages()?;

        Ok(store)
    }

    fn names_path(&self) -> PathBuf {
        self.dir.join("names.json")
    }

    fn load_names(&mut self) -> Result<()> {
        let path = self.names_path();
        if !path.exists() {
            return Ok(());
        }
        let content = std::fs::read(&path)?;
        let raw: HashMap<String, Vec<u8>> = serde_json::from_slice(&content)?;
        for (name, hash_vec) in raw {
            if hash_vec.len() == 32 {
                let mut key = [0u8; 32];
                key.copy_from_slice(&hash_vec);
                self.names.insert(name, key);
            }
        }
        Ok(())
    }

    fn save_names(&self) -> Result<()> {
        let raw: HashMap<String, Vec<u8>> = self
            .names
            .iter()
            .map(|(name, key)| (name.clone(), key.to_vec()))
            .collect();
        let content = serde_json::to_vec(&raw)?;
        std::fs::write(self.names_path(), content)?;
        Ok(())
    }

    fn load_packages(&mut self) -> Result<()> {
        let mut entries: Vec<_> = std::fs::read_dir(&self.dir)?
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
            let data = match std::fs::read(&path) {
                Ok(d) => Bytes::from(d),
                Err(_) => continue,
            };
            let key: HashKey = Sha256::digest(&data).into();

            if self.packages.contains_key(&key) {
                continue;
            }
            self.packages.insert(key, data.clone());

            if let Some(format) = detect_tar(&data) {
                let reader: Box<dyn Read> = if format == ".tar.gz" {
                    Box::new(GzDecoder::new(&data[..]))
                } else {
                    Box::new(Cursor::new(&data[..]))
                };
                if let Ok(entries) = Archive::new(reader).entries() {
                    for mut entry in entries.flatten() {
                        if let Ok(path) = entry.path() {
                            let path_str = path.to_string_lossy().into_owned();
                            let mut contents = Vec::new();
                            if entry.read_to_end(&mut contents).is_ok() {
                                self.cache.insert((key, path_str), contents.into());
                            }
                        }
                    }
                }
            }

            // Auto-register name from arcanum.toml
            if let Some(name) = self
                .cache
                .get(&(key, "arcanum.toml".into()))
                .and_then(|d| package_name_from_toml(d))
            {
                self.names.insert(name, key);
            }
        }

        // Persist any new names discovered from arcanum.toml
        let _ = self.save_names();

        Ok(())
    }
}

impl PackageStore for FileSystemPackageStore {
    fn resolve_name(&self, name: &str) -> Option<HashKey> {
        self.names.get(name).copied()
    }

    fn set_name(&mut self, name: &str, key: HashKey) {
        self.names.insert(name.to_string(), key);
        let _ = self.save_names();
    }

    fn get_package(&self, key: &HashKey) -> Option<Bytes> {
        self.packages.get(key).cloned()
    }

    fn add_package(&mut self, value: Bytes) -> Result<HashKey> {
        let key: HashKey = Sha256::digest(&value).into();

        if detect_tar(&value).is_none() {
            anyhow::bail!("Invalid tarball")
        }
        if self.packages.contains_key(&key) {
            anyhow::bail!("Package already exists")
        }

        let hash_hex: String = key.iter().map(|b| format!("{:02x}", b)).collect();
        let ext = if value.len() >= 2 && value[0] == 0x1f && value[1] == 0x8b {
            ".tar.gz"
        } else {
            ".tar"
        };
        let file_path = self.dir.join(format!("{}{}", hash_hex, ext));
        std::fs::write(&file_path, &value)?;

        self.packages.insert(key, value.clone());

        let format = detect_tar(&value).unwrap();
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

        // Auto-register name from arcanum.toml
        if let Some(name) = self
            .cache
            .get(&(key, "arcanum.toml".into()))
            .and_then(|d| package_name_from_toml(d))
        {
            self.set_name(&name, key);
        }

        Ok(key)
    }

    fn get_asset(&self, key: &HashKey, asset: &str) -> Option<Bytes> {
        self.cache.get(&(*key, asset.to_string())).cloned()
    }
    fn list_names(&self) -> Vec<String> {
        self.names.keys().cloned().collect()
    }
}

#[derive(serde::Deserialize)]
struct ArcanumPkg {
    name: Option<String>,
}

#[derive(serde::Deserialize)]
struct ArcanumToml {
    name: Option<String>,
    package: Option<ArcanumPkg>,
}

pub(crate) fn package_name_from_toml(data: &[u8]) -> Option<String> {
    let toml_str = String::from_utf8_lossy(data);
    let root: ArcanumToml = toml::from_str(&toml_str).ok()?;
    let raw = root.name.or_else(|| root.package.and_then(|p| p.name))?;
    if raw.starts_with('^') {
        Some(raw)
    } else {
        Some(format!("^{}", raw))
    }
}

pub(crate) fn detect_tar(data: &Bytes) -> Option<&'static str> {
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
