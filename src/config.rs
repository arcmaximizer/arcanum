use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

fn default_port() -> u16 {
    6202
}

fn default_bind() -> String {
    "127.0.0.1".into()
}

fn default_data_dir() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("arcanum")
}

fn default_config_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("arcanum")
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_bind")]
    pub bind: String,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            bind: default_bind(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataConfig {
    #[serde(default = "default_data_dir")]
    pub dir: PathBuf,
    #[serde(default)]
    pub packages_dir: Option<PathBuf>,
    #[serde(default)]
    pub auto_load_packages: bool,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            dir: default_data_dir(),
            packages_dir: None,
            auto_load_packages: false,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ArcanumConfig {
    #[serde(default)]
    pub http_server: HttpServerConfig,
    #[serde(default)]
    pub data: DataConfig,
}

impl ArcanumConfig {
    pub fn packages_dir(&self) -> PathBuf {
        self.data
            .packages_dir
            .clone()
            .unwrap_or_else(|| self.data.dir.join("packages"))
    }

    pub fn state_dir(&self) -> PathBuf {
        self.data.dir.join("state")
    }

    pub fn scheduler_db_path(&self) -> PathBuf {
        self.data.dir.join("scheduler.db")
    }

    pub fn store_db_path(&self) -> PathBuf {
        self.data.dir.join("store.db")
    }
}

#[derive(Parser, Debug)]
#[command(name = "arcanum", version, about = "Arcanum app runtime")]
pub struct CliArgs {
    #[arg(short = 'c', long, help = "Path to config file")]
    pub config: Option<PathBuf>,

    #[arg(short = 'd', long, help = "Data directory override")]
    pub data_dir: Option<PathBuf>,

    #[arg(long, help = "HTTP server port")]
    pub port: Option<u16>,

    #[arg(long, help = "HTTP server bind address")]
    pub bind: Option<String>,

    #[arg(long, help = "Directory of .tar.gz packages to auto-load")]
    pub packages_dir: Option<PathBuf>,

    #[arg(long, help = "Enable auto-loading packages from packages dir")]
    pub auto_load_packages: bool,
}

fn find_config(cli: &CliArgs) -> PathBuf {
    if let Some(path) = &cli.config {
        return path.clone();
    }
    let config_dir = default_config_dir();
    let candidates = [
        config_dir.join("config.toml"),
        config_dir.join("arcanum.toml"),
        PathBuf::from("arcanum.toml"),
    ];
    for c in &candidates {
        if c.exists() {
            return c.clone();
        }
    }
    candidates[0].clone()
}

pub fn load_config() -> (ArcanumConfig, CliArgs) {
    let cli = CliArgs::parse();

    let config_path = find_config(&cli);
    let mut config: ArcanumConfig = if config_path.exists() {
        let contents = std::fs::read_to_string(&config_path)
            .unwrap_or_else(|e| panic!("failed to read config {}: {}", config_path.display(), e));
        toml::from_str(&contents)
            .unwrap_or_else(|e| panic!("failed to parse config {}: {}", config_path.display(), e))
    } else {
        ArcanumConfig::default()
    };

    if let Some(dir) = &cli.data_dir {
        config.data.dir = dir.clone();
    }
    if let Some(port) = cli.port {
        config.http_server.port = port;
    }
    if let Some(bind) = &cli.bind {
        config.http_server.bind = bind.clone();
    }
    if let Some(dir) = &cli.packages_dir {
        config.data.packages_dir = Some(dir.clone());
    }
    if cli.auto_load_packages {
        config.data.auto_load_packages = true;
    }

    (config, cli)
}
