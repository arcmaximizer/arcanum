use std::io::Write;

use arcanum::config;
use arcanum::manager::ManagerHandle;
use arcanum::proc::http::HttpHandle;
use arcanum::proc::http_server::HttpServerHandle;
use arcanum::scheduler::{PersistentScheduler, Proposal, SchedulerHandle, run_scheduler};
use arcanum::store::{SqlitePackageStore, StoreHandle, load_packages_from_dir};
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use arcanum::types::ProcessId;

fn make_tar_gz(code: &str) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    {
        let mut ar = tar::Builder::new(&mut tar_bytes);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o644);
        header.set_size(code.len() as u64);
        header.set_path("main.lua").unwrap();
        header.set_cksum();
        ar.append(&header, code.as_bytes()).unwrap();
        ar.finish().unwrap();
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&tar_bytes).unwrap();
    encoder.finish().unwrap()
}

async fn register_app(store: &StoreHandle, namespace: &str, app: &str, code: &str) {
    let tarball = make_tar_gz(code);
    let key = store.add_package(tarball.into()).await.unwrap();
    let name = format!("^{}/{}", namespace, app);
    store.set_name(name, key);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let (config, _cli) = config::load_config();

    // Ensure data directories exist
    let data_dir = &config.data.dir;
    std::fs::create_dir_all(data_dir).expect("failed to create data directory");

    // Persistent package store
    let store_path = config.store_db_path();
    let store = StoreHandle::new(Box::new(
        SqlitePackageStore::open(&store_path).expect("failed to open store database"),
    ));

    // Persistent scheduler
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let manager = ManagerHandle::new(
        store.clone(),
        scheduler.clone(),
        config.state_dir(),
    );
    let persistent_scheduler =
        PersistentScheduler::open(config.scheduler_db_path()).expect("failed to open scheduler database");
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(persistent_scheduler),
        manager.clone(),
    ));

    // Auto-load packages from the packages directory
    let packages_dir = config.packages_dir();
    if config.data.auto_load_packages {
        match load_packages_from_dir(&store, &packages_dir).await {
            Ok(count) => tracing::info!("Loaded {} packages from {}", count, packages_dir.display()),
            Err(e) => tracing::warn!("Failed to load packages from {}: {}", packages_dir.display(), e),
        }
    }

    // Register demo apps (only if not already loaded from disk)
    if store.resolve_name("^arc/echo".into()).await.is_none() {
        register_app(
            &store,
            "arc",
            "echo",
            r#"return {
            entrypoint = function(ctx, value)
                return value
            end,
        }"#,
        )
        .await;
    }

    if store.resolve_name("^arc/hello".into()).await.is_none() {
        register_app(
            &store,
            "arc",
            "hello",
            r#"return {
            entrypoint = function(ctx, value)
                return call("^arc/echo/entrypoint", { message = "Hello world!" })
            end,
        }"#,
        )
        .await;
    }

    // Register sys/http as a stateless process
    let http_process = ProcessId {
        namespace: "sys".to_string(),
        app: "http".to_string(),
        proc: "entrypoint".to_string(),
    };

    let http = HttpHandle::new(scheduler.clone());
    manager.register_stateless(http_process.clone(), http.sender());

    // Start HTTP server (registers as ^sys/http-server)
    let server_process = ProcessId {
        namespace: "sys".to_string(),
        app: "http-server".to_string(),
        proc: "entrypoint".to_string(),
    };
    let server = HttpServerHandle::new(
        scheduler.clone(),
        config.http_server.port,
    )
    .await;
    manager.register_stateless(server_process, server.sender());

    // Submit initial proposals via scheduler
    let hello_process = ProcessId {
        namespace: "arc".to_string(),
        app: "hello".to_string(),
        proc: "entrypoint".to_string(),
    };

    fn msgpack_str(s: &str) -> Vec<u8> {
        rmp_serde::to_vec(&serde_json::Value::String(s.into())).unwrap_or_default()
    }

    scheduler
        .add_proposal(Proposal {
            process: hello_process.clone(),
            event: None,
            input: msgpack_str("start"),
            promise: None,
        })
        .await;

    // Keep running
    tokio::signal::ctrl_c().await.unwrap();
}
