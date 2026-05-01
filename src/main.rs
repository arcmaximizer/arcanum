mod executor;
mod scheduler;
mod state;
mod store;
mod types;

use scheduler::{InMemoryScheduler, SchedulerMsg};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::unbounded_channel::<SchedulerMsg>();

    let sch_data = Box::new(InMemoryScheduler::new());
    scheduler::run_scheduler(rx, sch_data).await;
}
