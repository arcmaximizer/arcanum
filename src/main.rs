mod executor;
mod scheduler;
mod state;
mod store;
mod types;

use scheduler::{InMemoryScheduler, SchedulerMsg};
use store::{InMemoryPackageStore, StoreMsg};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (scheduler_tx, scheduler_rx) = mpsc::unbounded_channel::<SchedulerMsg>();
    let (store_tx, store_rx) = mpsc::unbounded_channel::<StoreMsg>();

    let sch_data = Box::new(InMemoryScheduler::new());
    scheduler::run_scheduler(scheduler_rx, sch_data).await;

    let store_data = Box::new(InMemoryPackageStore::new());
    store::run_store(store_rx, store_data).await;
}
