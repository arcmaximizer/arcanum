mod executor;
mod scheduler;
mod state;
mod store;
mod types;

use executor::ExecutorHandle;
use scheduler::{InMemoryScheduler, Proposal, RuntimeCall, SchedulerHandle};
use state::{InMemoryKVState, StateHandle};
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use types::ProcessId;

async fn run_http_process(
    mut rx: mpsc::UnboundedReceiver<RuntimeCall>,
    scheduler: SchedulerHandle,
) {
    while let Some(call) = rx.recv().await {
        tracing::debug!("HTTP process: got request for {}", call.proposal.input);

        let response = format!("fetched: {}", call.proposal.input);

        let _ = scheduler.runtime_satisfy(call.proposal, response).await;
    }
}

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
    let (http_tx, http_rx) = mpsc::unbounded_channel::<RuntimeCall>();

    scheduler.register_runtime(http_process.clone(), http_tx);

    tokio::spawn(run_http_process(http_rx, scheduler.clone()));

    // Submit initial proposals via scheduler

    scheduler
        .add_proposal(Proposal {
            process: hello_process.clone(),
            event: None,
            input: "start".to_string(),
            promise: None,
        })
        .await;

    scheduler
        .add_proposal(Proposal {
            process: hello_process.clone(),
            event: None,
            input: "start".to_string(),
            promise: None,
        })
        .await;

    // Keep running
    tokio::signal::ctrl_c().await.unwrap();
}
