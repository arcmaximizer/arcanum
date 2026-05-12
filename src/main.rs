mod executor;
mod scheduler;
mod state;
mod store;
mod types;

use scheduler::{InMemoryScheduler, Proposal, SchedulerMsg};
use state::{InMemoryKVState, StateMsg};
use tokio::sync::mpsc;
use types::ProcessId;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let (scheduler_tx, scheduler_rx) = mpsc::unbounded_channel::<SchedulerMsg>();
    let (state_tx, state_rx) = mpsc::unbounded_channel::<StateMsg>();

    let sch_data = Box::new(InMemoryScheduler::new());
    tokio::spawn(scheduler::run_scheduler(scheduler_rx, sch_data));

    let state_data = InMemoryKVState::new();
    tokio::spawn(state::run_state(state_rx, state_data));

    // Create echo process
    let echo_process = ProcessId {
        app: "arc".to_string(),
        proc: "echo/entrypoint".to_string(),
    };
    let (echo_tx, echo_rx) = mpsc::unbounded_channel::<Proposal>();

    // Register echo executor with scheduler
    scheduler_tx
        .send(SchedulerMsg::RegisterExecutor {
            process: echo_process.clone(),
            tx: echo_tx.clone(),
        })
        .unwrap();

    tokio::spawn(executor::run_executor(
        echo_process.clone(),
        echo_rx,
        scheduler_tx.clone(),
        state_tx.clone(),
        r#"return function(value)
            return value
        end"#
            .to_string(),
    ));

    // Create hello process
    let hello_process = ProcessId {
        app: "arc".to_string(),
        proc: "hello/entrypoint".to_string(),
    };
    let (hello_tx, hello_rx) = mpsc::unbounded_channel::<Proposal>();

    scheduler_tx
        .send(SchedulerMsg::RegisterExecutor {
            process: hello_process.clone(),
            tx: hello_tx.clone(),
        })
        .unwrap();

    tokio::spawn(executor::run_executor(
        hello_process.clone(),
        hello_rx,
        scheduler_tx.clone(),
        state_tx.clone(),
        r#"return function(value)
            return call("^arc/echo/entrypoint", "Hello world!")
        end"#
            .to_string(),
    ));

    // Submit initial proposals via scheduler
    scheduler_tx
        .send(SchedulerMsg::AddProposal {
            proposal: Proposal {
                process: echo_process.clone(),
                event: None,
                input: "hello".to_string(),
                promise: None,
            },
            resp: tokio::sync::oneshot::channel().0,
        })
        .unwrap();

    scheduler_tx
        .send(SchedulerMsg::AddProposal {
            proposal: Proposal {
                process: hello_process,
                event: None,
                input: "start".to_string(),
                promise: None,
            },
            resp: tokio::sync::oneshot::channel().0,
        })
        .unwrap();

    // Keep running
    tokio::signal::ctrl_c().await.unwrap();
}
