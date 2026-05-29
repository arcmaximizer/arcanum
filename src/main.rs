mod conversions;
mod executor;
mod manager;
mod proc;
mod scheduler;
mod state;
mod store;
mod types;

use executor::ExecutorHandle;
use manager::ManagerHandle;
use proc::http::HttpHandle;
use proc::http_server::HttpServerHandle;
use scheduler::{InMemoryScheduler, Proposal, SchedulerHandle, run_scheduler};
use state::{InMemoryKVState, StateHandle};
use store::{InMemoryPackageStore, StoreHandle};
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use types::ProcessId;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

    // Create echo process
    let echo_process = ProcessId {
        namespace: "arc".to_string(),
        app: "echo".to_string(),
        proc: "entrypoint".to_string(),
    };

    let echo = ExecutorHandle::new(
        echo_process.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function(value)
            return value
        end"#
            .to_string(),
    );

    manager.register_executor(echo_process.clone(), echo.sender());

    // Create hello process
    let hello_process = ProcessId {
        namespace: "arc".to_string(),
        app: "hello".to_string(),
        proc: "entrypoint".to_string(),
    };

    let hello = ExecutorHandle::new(
        hello_process.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function(value)
            return call("^arc/echo/entrypoint", { message = "Hello world!" })
        end"#
            .to_string(),
    );

    manager.register_executor(hello_process.clone(), hello.sender());

    // Register sys/http as a stateless process
    let http_process = ProcessId {
        namespace: "sys".to_string(),
        app: "http".to_string(),
        proc: "entrypoint".to_string(),
    };

    let http = HttpHandle::new(scheduler.clone());
    manager.register_stateless(http_process.clone(), http.sender());

    // Start HTTP server on port 6202
    let _http_server = HttpServerHandle::new(scheduler.clone(), 6202).await;

    // Submit initial proposals via scheduler

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
