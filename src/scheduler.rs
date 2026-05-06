use crate::types::{EventId, ProcessId};
use anyhow::{Result, anyhow, bail};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum SchedulerMsg {
    AddProposal {
        proposal: Proposal,
        resp: tokio::sync::oneshot::Sender<u64>,
    },
    RegisterExecutor {
        process: ProcessId,
        tx: mpsc::UnboundedSender<Proposal>,
    },
    UnregisterExecutor {
        process: ProcessId,
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
    Satisfy {
        proposal: Proposal,
        receipt: Receipt,
        is_final: bool,
        resp: tokio::sync::oneshot::Sender<Result<NextAction>>,
    },
}

pub async fn run_scheduler(
    mut rx: mpsc::UnboundedReceiver<SchedulerMsg>,
    mut scheduler: Box<dyn Scheduler + Send>,
) {
    let mut executor_senders: HashMap<ProcessId, mpsc::UnboundedSender<Proposal>> = HashMap::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            SchedulerMsg::AddProposal { proposal, resp } => {
                let process = proposal.process.clone();
                let id = scheduler.add_proposal(proposal.clone());
                let _ = resp.send(id);
                if let Some(tx) = executor_senders.get(&process) {
                    let _ = tx.send(proposal);
                }
            }
            SchedulerMsg::RegisterExecutor { process, tx } => {
                executor_senders.insert(process, tx);
            }
            SchedulerMsg::UnregisterExecutor { process } => {
                executor_senders.remove(&process);
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
            SchedulerMsg::Satisfy {
                proposal,
                receipt,
                is_final,
                resp,
            } => {
                let result = scheduler.satisfy_proposal(&proposal, receipt, is_final);
                let _ = resp.send(result);
            }
        }
    }
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
    pub returns: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Syscall {
    KVRead { key: String, current_value: String },
    KVWrite { key: String, new_value: String },
    Call { proposal: Proposal },
    Notify { proposal: Proposal },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Proposal {
    pub process: ProcessId,
    pub event: Option<EventId>,
    pub input: String,
    pub promise: Option<Promise>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Promise {
    pub id: u64,
    pub target: EventId,
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
    fn satisfy_proposal(
        &mut self,
        proposal: &Proposal,
        receipt: Receipt,
        is_final: bool,
    ) -> Result<NextAction>;
    fn get_chunks_from_event(&self, event: &EventId) -> Option<&Vec<Receipt>>;
    fn get_chunk_from_event(&self, event: &EventId, chunk_seq: u64) -> Option<&Receipt>;
}

pub struct InMemoryScheduler {
    pub event_chunks: HashMap<EventId, Vec<Receipt>>,
    pub process_chunks: HashMap<ProcessId, Vec<Receipt>>,
    pub schedule: HashMap<ProcessId, VecDeque<Proposal>>,
    pub event_counter: HashMap<ProcessId, u64>,
    pub proposal_counter: u64,
}

impl InMemoryScheduler {
    pub fn new() -> Self {
        Self {
            event_chunks: HashMap::new(),
            process_chunks: HashMap::new(),
            schedule: HashMap::new(),
            event_counter: HashMap::new(),
            proposal_counter: 0,
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
            app: process.app.clone(),
            proc: process.proc.clone(),
            seq,
        }
    }
    fn satisfy_proposal(
        &mut self,
        proposal: &Proposal,
        receipt: Receipt,
        is_final: bool,
    ) -> Result<NextAction> {
        // This should always validate data to ensure that any state transitions always follow some
        // invariants. Keep effects at the end in case of reverts and use transactions if possible.

        // Extract values needed for validation before taking any mutable borrows
        let process = &proposal.process;
        let in_log_seq = receipt.in_log_seq;
        let in_event_seq = receipt.in_event_seq;
        let event_id = *self.event_counter.entry(process.clone()).or_insert(0);
        let event = proposal.event.clone().unwrap_or(EventId {
            app: process.app.clone(),
            proc: process.proc.clone(),
            seq: event_id,
        });

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
        let root_chunk = event_chunks.first().unwrap().clone();
        let promise = root_chunk.proposal.promise.clone();
        let root_returns = root_chunk.returns.clone();
        let promise_target = promise.as_ref().map(|p| p.target.clone());

        if is_final {
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

        for nt in notif_proposals {
            self.schedule
                .entry(nt.process.clone())
                .or_insert(VecDeque::new())
                .push_back(nt);
        }

        // Satisfy any existing promises
        if is_final {
            if let Some((source_event, source_process)) = source_chunk_data {
                self.add_proposal(Proposal {
                    event: Some(source_event),
                    process: source_process,
                    input: root_returns,
                    promise: None,
                });
            }
        }

        let action = NextAction {
            event,
            proposal: calls.last().cloned(),
        };

        Ok(action)
    }
    fn get_chunks_from_event(&self, event: &EventId) -> Option<&Vec<Receipt>> {
        self.event_chunks.get(event)
    }
    fn get_chunk_from_event(&self, event: &EventId, chunk_seq: u64) -> Option<&Receipt> {
        self.event_chunks.get(event)?.get(chunk_seq as usize)
    }
}
