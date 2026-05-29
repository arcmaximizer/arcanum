use crate::conversions::bytes_to_json_pretty;
use crate::manager::ManagerHandle;
use crate::types::{EventId, ProcessId};
use anyhow::{Result, anyhow, bail};
use std::collections::{HashMap, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tracing;

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
    RuntimeSatisfy {
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
            SchedulerMsg::RuntimeSatisfy {
                proposal,
                returns,
                resp,
            } => {
                let result = scheduler.runtime_satisfy(&proposal, returns);
                if let Ok((_action, new_proposals)) = &result {
                    if !new_proposals.is_empty() {
                        tracing::debug!(
                            "Adding {} new proposals from runtime_satisfy:",
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

    pub async fn runtime_satisfy(&self, proposal: Proposal, returns: Vec<u8>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(SchedulerMsg::RuntimeSatisfy {
                proposal,
                returns,
                resp: resp_tx,
            })
            .expect("Scheduler task has been killed");
        resp_rx.await.expect("Scheduler task has been killed")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RuntimeStatus {
    Normal,
    Error,
    End,
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Syscall {
    KVRead { key: String, current_value: String },
    KVWrite { key: String, new_value: String },
    SqlExec { sql: String },
    SqlQuery { sql: String },
    Call { proposal: Proposal },
    Notify { proposal: Proposal },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Proposal {
    pub process: ProcessId,
    pub event: Option<EventId>,
    pub input: Vec<u8>,
    pub promise: Option<Promise>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Promise {
    pub id: u64,
    pub target: EventId,
}

impl std::fmt::Display for Promise {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "P{} -> {}", self.id, self.target)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn runtime_satisfy(
        &mut self,
        proposal: &Proposal,
        returns: Vec<u8>,
    ) -> Result<(NextAction, Vec<Proposal>)>;
}

pub struct InMemoryScheduler {
    pub event_chunks: HashMap<EventId, Vec<Receipt>>,
    pub process_chunks: HashMap<ProcessId, Vec<Receipt>>,
    pub schedule: HashMap<ProcessId, VecDeque<Proposal>>,
    pub event_counter: HashMap<ProcessId, u64>,
}

impl InMemoryScheduler {
    pub fn new() -> Self {
        Self {
            event_chunks: HashMap::new(),
            process_chunks: HashMap::new(),
            schedule: HashMap::new(),
            event_counter: HashMap::new(),
        }
    }
}

impl Scheduler for InMemoryScheduler {
    fn add_proposal(&mut self, proposal: Proposal) -> u64 {
        let schedule = self
            .schedule
            .entry(proposal.process.clone())
            .or_insert(VecDeque::new());

        schedule.push_back(proposal);
        (schedule.len() - 1) as u64
    }
    fn get_next_proposal(&mut self, process: &ProcessId) -> Option<&Proposal> {
        self.schedule.get(process)?.get(0)
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

        let schedule = self
            .schedule
            .entry(process.clone())
            .or_insert(VecDeque::new());
        let event_chunks = self.event_chunks.entry(event.clone()).or_insert(vec![]);
        let process_chunks = self.process_chunks.entry(process.clone()).or_insert(vec![]);

        // Proposal checks
        let first_proposal = schedule
            .get(0)
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
        }

        // Parse the receipt's syscalls
        let mut calls = Vec::new();
        let mut notif_proposals = Vec::new();

        for syscall in receipt.syscalls.iter() {
            match syscall {
                Syscall::Call { proposal } => {
                    calls.push(proposal.clone());
                }
                Syscall::Notify { proposal } => notif_proposals.push(proposal.clone()),
                _ => {}
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
                .or_insert(VecDeque::new())
                .push_back(nt.clone());
            new_proposals.push(nt);
        }

        // Satisfy any existing promises
        if completes_proposal {
            if let Some((source_event, source_process)) = source_chunk_data {
                let promise_proposal = Proposal {
                    event: Some(source_event),
                    process: source_process,
                    input: root_returns,
                    promise: None,
                };
                self.add_proposal(promise_proposal.clone());
                new_proposals.push(promise_proposal);
            }
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

    fn runtime_satisfy(
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
            .get(0)
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
