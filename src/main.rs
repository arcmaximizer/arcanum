use arcanum::config::{self, CliArgs};
use arcanum::manager::ManagerHandle;
use arcanum::proc::http::HttpHandle;
use arcanum::proc::http_server::HttpServerHandle;
use arcanum::scheduler::{PersistentScheduler, SchedulerHandle, run_scheduler};
use arcanum::store::{FileSystemPackageStore, StoreHandle};
use arcanum::types::{AppId, ProcessId};
use clap::Parser;
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = CliArgs::parse();

    if let Some(config::Command::Shell {
        host,
        port,
        timeout,
        args,
    }) = cli.command
    {
        arcanum::shell::run(host, port, timeout, args).await;
        return;
    }

    let config = config::load_config(&cli);

    // Ensure data directories exist
    let data_dir = &config.data.dir;
    std::fs::create_dir_all(data_dir).expect("failed to create data directory");

    // Persistent package store (filesystem-backed)
    let store_dir = config.store_dir();
    let store = StoreHandle::new(Box::new(
        FileSystemPackageStore::open(&store_dir).expect("failed to open store directory"),
    ));

    // Persistent scheduler
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let manager = ManagerHandle::new(store.clone(), scheduler.clone(), config.state_dir());
    let persistent_scheduler = PersistentScheduler::open(config.scheduler_db_path())
        .expect("failed to open scheduler database");
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(persistent_scheduler),
        manager.clone(),
    ));

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
        server_process.clone(),
    )
    .await;
    manager.register_stateless(server_process, server.sender());

    // Start management server
    let mgmt = arcanum::mgmt::MgmtHandle::new(scheduler.clone(), config.mgmt.port).await;
    tracing::info!("management server listening on port {}", mgmt.port);

    // Spawn entrypoint for every package in the store
    for name in store.list_names().await {
        if let Ok(app_id) = AppId::try_from(name.as_str()) {
            let process = app_id.with_process("entrypoint".to_string());
            manager.spawn_actor(process);
        }
    }

    // Keep running
    tokio::signal::ctrl_c().await.unwrap();
}
