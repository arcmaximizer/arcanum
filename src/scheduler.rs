use crate::conversions::bytes_to_json_pretty;
use crate::manager::ManagerHandle;
use crate::types::{EventId, HandlerId, ProcessId};
use anyhow::{Result, anyhow, bail};
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum SchedulerMsg {
    AddProposal {
        proposal: Proposal,
        resp: tokio::sync::oneshot::Sender<u64>,
    },
    GetNext {
        process: ProcessId,
        resp: tokio::sync::oneshot::Sender<Option<Proposal>>,
    },
    GetChunks {
        event: EventId,
        resp: tokio::sync::oneshot::Sender<Option<Vec<Receipt>>>,
    },
    GetNextEventId {
        process: ProcessId,
        resp: tokio::sync::oneshot::Sender<EventId>,
    },
    GetLogSeq {
        process: ProcessId,
        resp: tokio::sync::oneshot::Sender<u64>,
    },
    Satisfy {
        proposal: Proposal,
        receipt: Receipt,
        completes_proposal: bool,
        resp: tokio::sync::oneshot::Sender<Result<()>>,
    },
    StatelessSatisfy {
        proposal: Proposal,
        returns: Vec<u8>,
        resp: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

pub async fn run_scheduler(
    mut rx: mpsc::UnboundedReceiver<SchedulerMsg>,
    mut scheduler: Box<dyn Scheduler + Send>,
    manager: ManagerHandle,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            SchedulerMsg::AddProposal { proposal, resp } => {
                let id = scheduler.add_proposal(proposal.clone());
                let _ = resp.send(id);

                manager.route_proposal(proposal);
            }
            SchedulerMsg::GetNext { process, resp } => {
                let proposal = scheduler.get_next_proposal(&process).cloned();
                let _ = resp.send(proposal);
            }
            SchedulerMsg::GetChunks { event, resp } => {
                let chunks = scheduler.get_chunks_from_event(&event).cloned();
                let _ = resp.send(chunks);
            }
            SchedulerMsg::GetNextEventId { process, resp } => {
                let event_id = scheduler.get_next_event_id(&process);
                let _ = resp.send(event_id);
            }
            SchedulerMsg::GetLogSeq { process, resp } => {
                let log_seq = scheduler.get_log_seq(&process);
                let _ = resp.send(log_seq);
            }
            SchedulerMsg::Satisfy {
                proposal,
                receipt,
                completes_proposal,
                resp,
            } => {
                let result = scheduler.satisfy_proposal(&proposal, receipt, completes_proposal);
                if let Ok((_action, new_proposals)) = &result {
                    if !new_proposals.is_empty() {
                        tracing::debug!(
                            "Adding {} new proposals from satisfy:",
                            new_proposals.len()
                        );
                        for p in new_proposals {
                            if let Some(ref promise) = p.promise {
                                tracing::debug!(
                                    "  -> process={} {} input={}",
                                    p.process,
                                    promise,
                                    bytes_to_json_pretty(&p.input)
                                );
                            } else {
                                tracing::debug!(
                                    "  -> process={} input={}",
                                    p.process,
                                    bytes_to_json_pretty(&p.input)
                                );
                            }
                        }
                    }
                    for p in new_proposals {
                        manager.route_proposal(p.clone());
                    }
                }
                let _ = resp.send(result.map(|_| ()));
            }
            SchedulerMsg::StatelessSatisfy {
                proposal,
                returns,
                resp,
            } => {
                let result = scheduler.stateless_satisfy(&proposal, returns);
                if let Ok((_action, new_proposals)) = &result {
                    if !new_proposals.is_empty() {
                        tracing::debug!(
                            "Adding {} new proposals from stateless_satisfy:",
                            new_proposals.len()
                        );
                        for p in new_proposals {
                            if let Some(ref promise) = p.promise {
                                tracing::debug!(
                                    "  -> process={} {} input={}",
                                    p.process,
                                    promise,
                                    bytes_to_json_pretty(&p.input)
                                );
                            } else {
                                tracing::debug!(
                                    "  -> process={} input={}",
                                    p.process,
                                    bytes_to_json_pretty(&p.input)
                                );
                            }
                        }
                    }
                    for p in new_proposals {
                        manager.route_proposal(p.clone());
                    }
                }
                let _ = resp.send(result.map(|_| ()));
            }
        }
    }
}

#[derive(Clone)]
pub struct SchedulerHandle {
    sender: mpsc::UnboundedSender<SchedulerMsg>,
}

impl SchedulerHandle {
    pub fn new(scheduler: Box<dyn Scheduler + Send>, manager: ManagerHandle) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_scheduler(receiver, scheduler, manager));
        Self { sender }
    }

    pub fn from_sender(sender: mpsc::UnboundedSender<SchedulerMsg>) -> Self {
        Self { sender }
    }

    pub async fn add_proposal(&self, proposal: Proposal) -> u64 {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(SchedulerMsg::AddProposal {
                proposal,
                resp: resp_tx,
            })
            .expect("Scheduler task has been killed");
        resp_rx.await.expect("Scheduler task has been killed")
    }

    pub async fn get_next(&self, process: ProcessId) -> Option<Proposal> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(SchedulerMsg::GetNext {
                process,
                resp: resp_tx,
            })
            .expect("Scheduler task has been killed");
        resp_rx.await.expect("Scheduler task has been killed")
    }

    pub async fn get_chunks(&self, event: EventId) -> Option<Vec<Receipt>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(SchedulerMsg::GetChunks {
                event,
                resp: resp_tx,
            })
            .expect("Scheduler task has been killed");
        resp_rx.await.expect("Scheduler task has been killed")
    }

    pub async fn get_next_event_id(&self, process: ProcessId) -> EventId {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(SchedulerMsg::GetNextEventId {
                process,
                resp: resp_tx,
            })
            .expect("Scheduler task has been killed");
        resp_rx.await.expect("Scheduler task has been killed")
    }

    pub async fn get_log_seq(&self, process: ProcessId) -> u64 {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(SchedulerMsg::GetLogSeq {
                process,
                resp: resp_tx,
            })
            .expect("Scheduler task has been killed");
        resp_rx.await.expect("Scheduler task has been killed")
    }

    pub async fn satisfy(
        &self,
        proposal: Proposal,
        receipt: Receipt,
        completes_proposal: bool,
    ) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(SchedulerMsg::Satisfy {
                proposal,
                receipt,
                completes_proposal,
                resp: resp_tx,
            })
            .expect("Scheduler task has been killed");
        resp_rx.await.expect("Scheduler task has been killed")
    }

    pub async fn stateless_satisfy(&self, proposal: Proposal, returns: Vec<u8>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(SchedulerMsg::StatelessSatisfy {
                proposal,
                returns,
                resp: resp_tx,
            })
            .expect("Scheduler task has been killed");
        resp_rx.await.expect("Scheduler task has been killed")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RuntimeStatus {
    Normal,
    Error,
    End,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Receipt {
    pub proposal: Proposal,
    /// Sequence index inside a given event's history
    /// e.g. ^arc/my-app/my-process/e48/c1
    pub in_event_seq: u64,
    /// Sequence index inside the chunk history
    /// e.g. ^arc/my-app/my-process/c48
    pub in_log_seq: u64,
    pub syscalls: Vec<Syscall>,
    pub returns: Vec<u8>,
    pub status: RuntimeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Syscall {
    KVRead {
        key: String,
        current_value: String,
    },
    KVWrite {
        key: String,
        new_value: String,
    },
    SqlExec {
        sql: String,
        params: Vec<u8>,
    },
    SqlQuery {
        sql: String,
        params: Vec<u8>,
    },
    Call {
        proposal: Proposal,
    },
    Notify {
        proposal: Proposal,
    },
    Register {
        process: ProcessId,
        handler: HandlerId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Proposal {
    pub process: ProcessId,
    pub event: Option<EventId>,
    pub input: Vec<u8>,
    pub promise: Option<Promise>,
    pub from: ProcessId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Promise {
    pub id: u64,
    pub target: EventId,
}

impl std::fmt::Display for Promise {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "P{} -> {}", self.id, self.target)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NextAction {
    pub event: EventId,
    pub proposal: Option<Proposal>,
}

pub trait Scheduler {
    fn add_proposal(&mut self, proposal: Proposal) -> u64;
    fn get_next_proposal(&mut self, process: &ProcessId) -> Option<&Proposal>;
    fn get_next_event_id(&mut self, process: &ProcessId) -> EventId;
    fn get_log_seq(&self, process: &ProcessId) -> u64;
    fn satisfy_proposal(
        &mut self,
        proposal: &Proposal,
        receipt: Receipt,
        completes_proposal: bool,
    ) -> Result<(NextAction, Vec<Proposal>)>;
    fn get_chunks_from_event(&self, event: &EventId) -> Option<&Vec<Receipt>>;
    fn get_chunk_from_event(&self, event: &EventId, chunk_seq: u64) -> Option<&Receipt>;
    fn stateless_satisfy(
        &mut self,
        proposal: &Proposal,
        returns: Vec<u8>,
    ) -> Result<(NextAction, Vec<Proposal>)>;
}

#[derive(Default)]
pub struct InMemoryScheduler {
    pub event_chunks: HashMap<EventId, Vec<Receipt>>,
    pub process_chunks: HashMap<ProcessId, Vec<Receipt>>,
    pub schedule: HashMap<ProcessId, VecDeque<Proposal>>,
    pub event_counter: HashMap<ProcessId, u64>,
}

impl InMemoryScheduler {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Scheduler for InMemoryScheduler {
    fn add_proposal(&mut self, proposal: Proposal) -> u64 {
        let schedule = self.schedule.entry(proposal.process.clone()).or_default();

        schedule.push_back(proposal);
        (schedule.len() - 1) as u64
    }
    fn get_next_proposal(&mut self, process: &ProcessId) -> Option<&Proposal> {
        self.schedule.get(process)?.front()
    }
    fn get_next_event_id(&mut self, process: &ProcessId) -> EventId {
        let seq = *self.event_counter.entry(process.clone()).or_insert(0);
        EventId {
            namespace: process.namespace.clone(),
            app: process.app.clone(),
            proc: process.proc.clone(),
            seq,
        }
    }
    fn get_log_seq(&self, process: &ProcessId) -> u64 {
        self.process_chunks
            .get(process)
            .map(|c| c.len() as u64)
            .unwrap_or(0)
    }
    fn satisfy_proposal(
        &mut self,
        proposal: &Proposal,
        receipt: Receipt,
        completes_proposal: bool,
    ) -> Result<(NextAction, Vec<Proposal>)> {
        // This should always validate data to ensure that any state transitions always follow some
        // invariants. Keep effects at the end in case of reverts and use transactions if possible.

        // Extract values needed for validation before taking any mutable borrows
        let process = &proposal.process;
        let in_log_seq = receipt.in_log_seq;
        let in_event_seq = receipt.in_event_seq;
        let event = if let Some(ref e) = proposal.event {
            e.clone()
        } else if in_event_seq == 0 {
            let event_id = *self.event_counter.entry(process.clone()).or_insert(0);
            self.event_counter.insert(process.clone(), event_id + 1);
            EventId {
                namespace: process.namespace.clone(),
                app: process.app.clone(),
                proc: process.proc.clone(),
                seq: event_id,
            }
        } else {
            // Reuse the event assigned on the first chunk (in_event_seq == 0)
            let event_id = *self.event_counter.entry(process.clone()).or_insert(0);
            EventId {
                namespace: process.namespace.clone(),
                app: process.app.clone(),
                proc: process.proc.clone(),
                seq: event_id.saturating_sub(1),
            }
        };

        let schedule = self.schedule.entry(process.clone()).or_default();
        let event_chunks = self.event_chunks.entry(event.clone()).or_insert(vec![]);
        let process_chunks = self.process_chunks.entry(process.clone()).or_insert(vec![]);

        // Proposal checks
        let first_proposal = schedule
            .front()
            .ok_or(anyhow!("No proposals exist in schedule"))?
            .clone();

        if *proposal != first_proposal {
            bail!("Proposal does not match first scheduled proposal")
        }

        if in_log_seq != process_chunks.len() as u64 {
            bail!("Chunk is misaligned with chunk history")
        }

        // Event checks
        if in_event_seq != event_chunks.len() as u64 {
            bail!("Chunk is misaligned with per-event log")
        }

        if in_event_seq != 0 {
            let prev_receipt = event_chunks.get((in_event_seq - 1) as usize).unwrap();

            if prev_receipt.proposal.process != receipt.proposal.process {
                bail!("Chunk process is mismatched with previous chunk")
            }

            // Invariant 4: if the previous chunk belongs to a different proposal
            // (event was suspended by a Call and resumed via promise), then the
            // previous chunk must end with a Call syscall
            if prev_receipt.proposal != *proposal {
                let prev_ends_with_call = prev_receipt
                    .syscalls
                    .last()
                    .is_some_and(|s| matches!(s, Syscall::Call { .. }));
                if !prev_ends_with_call {
                    bail!("Previous chunk must end with a Call syscall");
                }
            }
        }

        // Parse the receipt's syscalls
        let mut calls = Vec::new();
        let mut notif_proposals = Vec::new();

        for (i, syscall) in receipt.syscalls.iter().enumerate() {
            match syscall {
                Syscall::Call { proposal } => {
                    calls.push(proposal.clone());
                }
                Syscall::Notify { proposal } => notif_proposals.push(proposal.clone()),
                _ => {}
            }

            // Invariant 7: Call must be the last syscall in the list
            if matches!(syscall, Syscall::Call { .. }) && i != receipt.syscalls.len() - 1 {
                bail!("Call syscall must be the last syscall in the list");
            }
        }

        if calls.len() > 1 {
            bail!("Only one Call syscall is allowed");
        }

        // Get root chunk for promise resolution
        let root_chunk = if in_event_seq == 0 {
            receipt.clone()
        } else {
            event_chunks.first().unwrap().clone()
        };
        let promise = root_chunk.proposal.promise.clone();
        let root_returns = root_chunk.returns.clone();
        let promise_target = promise.as_ref().map(|p| p.target.clone());

        if completes_proposal {
            schedule.pop_front();
        }

        event_chunks.push(receipt.clone());

        self.process_chunks
            .entry(process.clone())
            .or_insert(vec![])
            .push(receipt);

        let source_chunk_data = if let Some(ref target) = promise_target {
            self.event_chunks
                .get(target)
                .and_then(|chunks| chunks.first())
                .map(|chunk| (target.clone(), chunk.proposal.process.clone()))
        } else {
            None
        };

        let mut new_proposals = Vec::new();

        for nt in notif_proposals {
            self.schedule
                .entry(nt.process.clone())
                .or_default()
                .push_back(nt.clone());
            new_proposals.push(nt);
        }

        // Satisfy any existing promises
        if completes_proposal && let Some((source_event, source_process)) = source_chunk_data {
            let promise_proposal = Proposal {
                event: Some(source_event),
                process: source_process,
                input: root_returns,
                promise: None,
                from: proposal.process.clone(),
            };
            self.add_proposal(promise_proposal.clone());
            new_proposals.push(promise_proposal);
        }

        let action = NextAction {
            event,
            proposal: calls.last().cloned(),
        };

        Ok((action, new_proposals))
    }
    fn get_chunks_from_event(&self, event: &EventId) -> Option<&Vec<Receipt>> {
        self.event_chunks.get(event)
    }
    fn get_chunk_from_event(&self, event: &EventId, chunk_seq: u64) -> Option<&Receipt> {
        self.event_chunks.get(event)?.get(chunk_seq as usize)
    }

    fn stateless_satisfy(
        &mut self,
        proposal: &Proposal,
        returns: Vec<u8>,
    ) -> Result<(NextAction, Vec<Proposal>)> {
        let process = &proposal.process;

        let schedule = self
            .schedule
            .get_mut(process)
            .ok_or(anyhow!("No schedule exists for process {}", process))?;

        let first_proposal = schedule
            .front()
            .ok_or(anyhow!("No proposals exist in schedule"))?
            .clone();

        if *proposal != first_proposal {
            bail!("Proposal does not match first scheduled proposal")
        }

        schedule.pop_front();

        let promise = proposal.promise.clone();
        let promise_target = promise.as_ref().map(|p| p.target.clone());

        let source_chunk_data = if let Some(ref target) = promise_target {
            self.event_chunks
                .get(target)
                .and_then(|chunks| chunks.first())
                .map(|chunk| (target.clone(), chunk.proposal.process.clone()))
        } else {
            None
        };

        let mut new_proposals = Vec::new();

        if let Some((source_event, source_process)) = source_chunk_data {
            let promise_proposal = Proposal {
                event: Some(source_event),
                process: source_process,
                input: returns,
                promise: None,
                from: proposal.process.clone(),
            };
            self.add_proposal(promise_proposal.clone());
            new_proposals.push(promise_proposal);
        }

        let action = NextAction {
            event: EventId {
                namespace: process.namespace.clone(),
                app: process.app.clone(),
                proc: process.proc.clone(),
                seq: 0,
            },
            proposal: None,
        };

        Ok((action, new_proposals))
    }
}

/// A scheduler that wraps InMemoryScheduler and persists all state to SQLite.
/// On creation, it restores previous state from the database.
pub struct PersistentScheduler {
    inner: InMemoryScheduler,
    conn: rusqlite::Connection,
}

impl PersistentScheduler {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = rusqlite::Connection::open(path.as_ref())?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS proposals (
                process_namespace TEXT NOT NULL,
                process_app TEXT NOT NULL,
                process_proc TEXT NOT NULL,
                position INTEGER NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (process_namespace, process_app, process_proc, position)
            );
            CREATE TABLE IF NOT EXISTS event_chunks (
                event_namespace TEXT NOT NULL,
                event_app TEXT NOT NULL,
                event_proc TEXT NOT NULL,
                event_seq INTEGER NOT NULL,
                chunk_seq INTEGER NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (event_namespace, event_app, event_proc, event_seq, chunk_seq)
            );
            CREATE TABLE IF NOT EXISTS process_chunks (
                process_namespace TEXT NOT NULL,
                process_app TEXT NOT NULL,
                process_proc TEXT NOT NULL,
                chunk_seq INTEGER NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (process_namespace, process_app, process_proc, chunk_seq)
            );
            CREATE TABLE IF NOT EXISTS event_counters (
                process_namespace TEXT NOT NULL,
                process_app TEXT NOT NULL,
                process_proc TEXT NOT NULL,
                counter INTEGER NOT NULL,
                PRIMARY KEY (process_namespace, process_app, process_proc)
            );",
        )?;

        let mut scheduler = Self {
            inner: InMemoryScheduler::default(),
            conn,
        };
        scheduler.restore()?;
        Ok(scheduler)
    }

    fn restore(&mut self) -> Result<()> {
        // Restore event counters
        {
            let mut stmt = self
                .conn
                .prepare("SELECT process_namespace, process_app, process_proc, counter FROM event_counters")?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                ))
            })?;
            for row in rows {
                let (ns, app, proc_name, counter) = row?;
                let pid = ProcessId {
                    namespace: ns,
                    app,
                    proc: proc_name,
                };
                self.inner.event_counter.insert(pid, counter);
            }
        }

        // Restore proposals (re-add in order)
        {
            let mut stmt = self.conn.prepare(
                "SELECT process_namespace, process_app, process_proc, position, data
                 FROM proposals ORDER BY process_namespace, process_app, process_proc, position",
            )?;
            let rows = stmt.query_map([], |row| {
                let data: Vec<u8> = row.get(4)?;
                Ok(data)
            })?;
            for row in rows {
                let data: Vec<u8> = row?;
                if let Ok(proposal) = rmp_serde::from_slice::<Proposal>(&data) {
                    self.inner
                        .schedule
                        .entry(proposal.process.clone())
                        .or_default()
                        .push_back(proposal);
                }
            }
        }

        // Restore event chunks
        {
            let mut stmt = self.conn.prepare(
                "SELECT event_namespace, event_app, event_proc, event_seq, chunk_seq, data
                 FROM event_chunks ORDER BY event_namespace, event_app, event_proc, event_seq, chunk_seq",
            )?;
            let rows = stmt.query_map([], |row| {
                let event_ns: String = row.get(0)?;
                let event_app: String = row.get(1)?;
                let event_proc: String = row.get(2)?;
                let event_seq: u64 = row.get(3)?;
                let data: Vec<u8> = row.get(5)?;
                Ok((event_ns, event_app, event_proc, event_seq, data))
            })?;
            for row in rows {
                let (event_ns, event_app, event_proc, event_seq, data) = row?;
                let event = EventId {
                    namespace: event_ns,
                    app: event_app,
                    proc: event_proc,
                    seq: event_seq,
                };
                if let Ok(receipt) = rmp_serde::from_slice::<Receipt>(&data) {
                    self.inner
                        .event_chunks
                        .entry(event)
                        .or_default()
                        .push(receipt);
                }
            }
        }

        // Restore process chunks
        {
            let mut stmt = self.conn.prepare(
                "SELECT process_namespace, process_app, process_proc, chunk_seq, data
                 FROM process_chunks ORDER BY process_namespace, process_app, process_proc, chunk_seq",
            )?;
            let rows = stmt.query_map([], |row| {
                let proc_ns: String = row.get(0)?;
                let proc_app: String = row.get(1)?;
                let proc_name: String = row.get(2)?;
                let data: Vec<u8> = row.get(4)?;
                Ok((proc_ns, proc_app, proc_name, data))
            })?;
            for row in rows {
                let (proc_ns, proc_app, proc_name, data) = row?;
                let pid = ProcessId {
                    namespace: proc_ns,
                    app: proc_app,
                    proc: proc_name,
                };
                if let Ok(receipt) = rmp_serde::from_slice::<Receipt>(&data) {
                    self.inner
                        .process_chunks
                        .entry(pid)
                        .or_default()
                        .push(receipt);
                }
            }
        }

        tracing::info!(
            "Restored scheduler: {} event counters, {} proposals, {} event chunks, {} process chunks",
            self.inner.event_counter.len(),
            self.inner.schedule.values().map(|v| v.len()).sum::<usize>(),
            self.inner.event_chunks.len(),
            self.inner.process_chunks.len(),
        );

        Ok(())
    }

    fn save_proposal(&self, process: &ProcessId, position: u64, proposal: &Proposal) {
        if let Ok(data) = rmp_serde::to_vec(proposal) {
            let _ = self.conn.execute(
                "INSERT OR REPLACE INTO proposals (process_namespace, process_app, process_proc, position, data)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    process.namespace,
                    process.app,
                    process.proc,
                    position as i64,
                    data,
                ],
            );
        }
    }

    fn remove_proposal(&self, process: &ProcessId, position: i64) {
        let _ = self.conn.execute(
            "DELETE FROM proposals WHERE process_namespace = ?1 AND process_app = ?2 AND process_proc = ?3 AND position >= ?4",
            rusqlite::params![process.namespace, process.app, process.proc, position],
        );
    }

    fn save_receipt(&self, event: &EventId, chunk_seq: u64, receipt: &Receipt) {
        if let Ok(data) = rmp_serde::to_vec(receipt) {
            let _ = self.conn.execute(
                "INSERT OR REPLACE INTO event_chunks (event_namespace, event_app, event_proc, event_seq, chunk_seq, data)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    event.namespace,
                    event.app,
                    event.proc,
                    event.seq as i64,
                    chunk_seq as i64,
                    data,
                ],
            );
        }
    }

    fn save_process_receipt(&self, process: &ProcessId, chunk_seq: u64, receipt: &Receipt) {
        if let Ok(data) = rmp_serde::to_vec(receipt) {
            let _ = self.conn.execute(
                "INSERT OR REPLACE INTO process_chunks (process_namespace, process_app, process_proc, chunk_seq, data)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    process.namespace,
                    process.app,
                    process.proc,
                    chunk_seq as i64,
                    data,
                ],
            );
        }
    }

    fn save_event_counter(&self, process: &ProcessId, counter: u64) {
        let _ = self.conn.execute(
            "INSERT OR REPLACE INTO event_counters (process_namespace, process_app, process_proc, counter)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                process.namespace,
                process.app,
                process.proc,
                counter as i64,
            ],
        );
    }
}

impl Scheduler for PersistentScheduler {
    fn add_proposal(&mut self, proposal: Proposal) -> u64 {
        let position = self.inner.add_proposal(proposal.clone());
        self.save_proposal(&proposal.process, position, &proposal);
        position
    }

    fn get_next_proposal(&mut self, process: &ProcessId) -> Option<&Proposal> {
        self.inner.get_next_proposal(process)
    }

    fn get_next_event_id(&mut self, process: &ProcessId) -> EventId {
        let event_id = self.inner.get_next_event_id(process);
        if let Some(&counter) = self.inner.event_counter.get(process) {
            self.save_event_counter(process, counter);
        }
        event_id
    }

    fn get_log_seq(&self, process: &ProcessId) -> u64 {
        self.inner.get_log_seq(process)
    }

    fn satisfy_proposal(
        &mut self,
        proposal: &Proposal,
        receipt: Receipt,
        completes_proposal: bool,
    ) -> Result<(NextAction, Vec<Proposal>)> {
        let process = &proposal.process;
        // Determine event before mutation for saving
        let in_event_seq = receipt.in_event_seq;
        let _event = if let Some(ref e) = proposal.event {
            e.clone()
        } else if in_event_seq == 0 {
            let event_id = *self.inner.event_counter.entry(process.clone()).or_insert(0);
            self.inner.event_counter.insert(process.clone(), event_id + 1);
            self.save_event_counter(process, event_id + 1);
            EventId {
                namespace: process.namespace.clone(),
                app: process.app.clone(),
                proc: process.proc.clone(),
                seq: event_id,
            }
        } else {
            let event_id = *self.inner.event_counter.entry(process.clone()).or_insert(0);
            EventId {
                namespace: process.namespace.clone(),
                app: process.app.clone(),
                proc: process.proc.clone(),
                seq: event_id.saturating_sub(1),
            }
        };

        let chunk_seq = receipt.in_log_seq;
        let result = self
            .inner
            .satisfy_proposal(proposal, receipt, completes_proposal);

        if let Ok((ref action, ref new_proposals)) = result {
            if let Some(last) = self.inner.event_chunks.get(&action.event).and_then(|v| v.last()) {
                self.save_receipt(&action.event, chunk_seq, last);
            }
            if let Some(last) = self.inner.process_chunks.get(process).and_then(|v| v.last()) {
                self.save_process_receipt(process, chunk_seq, last);
            }

            if completes_proposal {
                // Remove completed proposal from persisted schedule
                let schedule = self.inner.schedule.get(process);
                let _removed_count = schedule.map(|s| s.len() as i64).unwrap_or(0);
                self.remove_proposal(process, 0);
                // Re-save remaining proposals
                if let Some(sched) = self.inner.schedule.get(process) {
                    for (i, p) in sched.iter().enumerate() {
                        self.save_proposal(process, i as u64, p);
                    }
                }
            }

            for p in new_proposals {
                if p.promise.is_some() {
                    // Promise proposals get added via add_proposal which already persists
                } else {
                    // Notifications: persist as new proposals
                    let sched = self.inner.schedule.get(&p.process);
                    let pos = sched.map(|s| (s.len() as u64).saturating_sub(1)).unwrap_or(0);
                    self.save_proposal(&p.process, pos, p);
                }
            }
        }

        result
    }

    fn get_chunks_from_event(&self, event: &EventId) -> Option<&Vec<Receipt>> {
        self.inner.get_chunks_from_event(event)
    }

    fn get_chunk_from_event(&self, event: &EventId, chunk_seq: u64) -> Option<&Receipt> {
        self.inner.get_chunk_from_event(event, chunk_seq)
    }

    fn stateless_satisfy(
        &mut self,
        proposal: &Proposal,
        returns: Vec<u8>,
    ) -> Result<(NextAction, Vec<Proposal>)> {
        let process = &proposal.process;
        let result = self.inner.stateless_satisfy(proposal, returns);

        if let Ok((_, ref new_proposals)) = result {
            if let Some(sched) = self.inner.schedule.get(process) {
                self.remove_proposal(process, 0);
                for (i, p) in sched.iter().enumerate() {
                    self.save_proposal(process, i as u64, p);
                }
            }
            for p in new_proposals {
                let sched = self.inner.schedule.get(&p.process);
                let pos = sched.map(|s| (s.len() as u64).saturating_sub(1)).unwrap_or(0);
                self.save_proposal(&p.process, pos, p);
            }
        }

        result
    }
}
