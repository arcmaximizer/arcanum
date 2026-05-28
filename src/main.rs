mod executor;
mod proc;
mod scheduler;
mod state;
mod store;
mod types;

use executor::ExecutorHandle;
use proc::http::HttpHandle;
use scheduler::{InMemoryScheduler, Proposal, SchedulerHandle};
use state::{InMemoryKVState, StateHandle};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use types::ProcessId;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let scheduler = SchedulerHandle::new(Box::new(InMemoryScheduler::new()));
    let state = StateHandle::new(InMemoryKVState::new());

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

    scheduler.register_executor(echo_process.clone(), echo.sender());

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
            return call("^arc/echo/entrypoint", "Hello world!")
        end"#
            .to_string(),
    );

    scheduler.register_executor(hello_process.clone(), hello.sender());

    // Register sys/http as a runtime process
    let http_process = ProcessId {
        namespace: "sys".to_string(),
        app: "http".to_string(),
        proc: "runtime".to_string(),
    };

    let http = HttpHandle::new(scheduler.clone());
    scheduler.register_runtime(http_process.clone(), http.sender());

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
