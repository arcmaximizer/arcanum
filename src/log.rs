use anyhow::{bail, Result};
use std::collections::{BTreeMap, HashMap, VecDeque};

pub trait Log {
    /// Adds a chunk to the log. Returns the chunk's log_seq on success.
    fn add_chunk(&mut self, chunk: Chunk) -> Result<u64>;
    /// Adds an event to the log. Returns the event's seq on success.
    fn add_event(&mut self, event: Event) -> Result<u64>;
    /// Gets a chunk by event ID and chunk sequence number within that event.
    fn get_chunk_in_event(&self, event_id: EventId, chunk_seq: u64) -> Option<Chunk>;
    /// Gets a chunk by app_id, proc_id, and log sequence number.
    fn get_chunk_in_log(&self, app_id: String, proc_id: String, log_seq: u64) -> Option<Chunk>;
    /// Gets all chunks for a given event.
    fn get_chunks_in_event(&self, event_id: EventId) -> Option<Vec<Chunk>>;
    /// Gets all running events across all processes.
    fn get_running_events(&self) -> Option<Vec<Event>>;
    /// Gets all queued events for a given process.
    fn get_queued_events(&self, app_id: String, proc_id: String) -> Option<Vec<Event>>;
}

pub struct InMemoryLog {
    chunks: HashMap<ProcessId, Vec<Chunk>>,
    events: HashMap<ProcessId, Vec<Event>>,
    event_id_to_chunks: HashMap<EventId, Vec<u64>>,
    queue: HashMap<ProcessId, VecDeque<EventId>>,
    running: BTreeMap<u64, EventId>,
}

impl Log for InMemoryLog {
    fn add_chunk(&mut self, chunk: Chunk) -> Result<u64> {
        let process = &chunk.event_id.proc;

        let events = self.events.entry(process.clone()).or_insert_with(Vec::new);
        let process_chunk_log = self.chunks.entry(process.clone()).or_insert_with(Vec::new);
        let event_chunk_log = self
            .event_id_to_chunks
            .entry(chunk.event_id.clone())
            .or_insert_with(Vec::new);

        if chunk.event_id.seq as usize > events.len() {
            bail!("no event")
        }

        let event = &mut events[chunk.event_id.seq as usize];

        if chunk.log_seq != process_chunk_log.len() as u64 {
            bail!("chunk ID mismatch")
        }

        if chunk.chunk_seq != event_chunk_log.len() as u64 {
            bail!("chunk seq mismatch")
        }

        if let EventStatus::Completed(_) = event.status {
            bail!("event completed")
        }

        if chunk.chunk_seq == 0 {
            let queue = self
                .queue
                .entry(chunk.event_id.proc.clone())
                .or_insert_with(VecDeque::new);
            queue.retain(|e| e.seq != chunk.event_id.seq);
            self.running
                .insert(chunk.event_id.seq, chunk.event_id.clone());
        }

        if let ChunkStatus::End(data) = chunk.status.clone() {
            event.status = EventStatus::Completed(data);
            self.running.remove(&chunk.event_id.seq);
        }

        let seq = chunk.log_seq;

        event_chunk_log.push(chunk.log_seq);
        process_chunk_log.push(chunk);
        Ok(seq)
    }
    fn add_event(&mut self, event: Event) -> Result<u64> {
        let process = &event.id.proc;
        let events = self.events.entry(process.clone()).or_insert_with(Vec::new);

        if event.id.seq != events.len() as u64 {
            bail!("event ID mismatch")
        }

        match &event.status {
            EventStatus::Pending => {
                self.queue
                    .entry(event.id.proc.clone())
                    .or_insert_with(VecDeque::new)
                    .push_back(event.id.clone());
            }
            EventStatus::Running => {
                self.running.insert(event.id.seq, event.id.clone());
            }
            _ => {}
        }

        events.push(event.clone());
        Ok(event.id.seq)
    }
    fn get_chunk_in_event(&self, event_id: EventId, chunk_seq: u64) -> Option<Chunk> {
        let chunk_seqs = self.event_id_to_chunks.get(&event_id)?;
        let &log_seq = chunk_seqs.get(chunk_seq as usize)?;
        let process = &event_id.proc;
        let chunks = self.chunks.get(process)?;
        chunks.get(log_seq as usize).cloned()
    }
    fn get_chunk_in_log(&self, app_id: String, proc_id: String, log_seq: u64) -> Option<Chunk> {
        let process = ProcessId {
            app: app_id,
            proc: proc_id,
        };
        let chunks = self.chunks.get(&process)?;
        chunks.get(log_seq as usize).cloned()
    }
    fn get_chunks_in_event(&self, event_id: EventId) -> Option<Vec<Chunk>> {
        let chunk_seqs = self.event_id_to_chunks.get(&event_id)?;
        let process = &event_id.proc;
        let chunks = self.chunks.get(process)?;
        let result: Vec<Chunk> = chunk_seqs
            .iter()
            .filter_map(|&log_seq| chunks.get(log_seq as usize).cloned())
            .collect();
        Some(result)
    }
    fn get_running_events(&self) -> Option<Vec<Event>> {
        let result: Vec<Event> = self
            .running
            .values()
            .filter_map(|event_id| {
                let events = self.events.get(&event_id.proc)?;
                events.get(event_id.seq as usize).cloned()
            })
            .collect();
        Some(result)
    }
    fn get_queued_events(&self, app_id: String, proc_id: String) -> Option<Vec<Event>> {
        let process = ProcessId {
            app: app_id,
            proc: proc_id,
        };
        let queue = self.queue.get(&process)?;
        let events = self.events.get(&process)?;
        let result: Vec<Event> = queue
            .iter()
            .filter_map(|event_id| events.get(event_id.seq as usize).cloned())
            .collect();
        Some(result)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    /// ID of the event this chunk falls under
    /// e.g. hello-world/greeter/e5
    pub event_id: EventId,
    /// Chunk's position within our current execution
    /// e.g. hello-world/greeter/e5/c0
    pub chunk_seq: u64,
    /// Chunk's position in the chunk log of app/proc.
    /// e.g. hello-world/greeter/c115
    /// Note that we do not have a single global log for all apps/processes, so
    /// this is NOT the position of this relative to every other chunk,
    /// including those from other processes.
    pub log_seq: u64,
    /// Whether this chunk marks an end to the event or not
    /// Invariant: after a chunk where completed=true, no new chunks will be
    /// appended to the log of a given event
    pub status: ChunkStatus,
    /// Any inputs returned to system calls. These are used during replay to get
    /// our execution the values it needs, so anything possibly indeterministic
    /// is tracked and replayed.
    pub inputs: Vec<ChunkInput>,
    /// Any outgoing messages, for instance to other processes or to runtime
    /// extensions.
    pub effects: Vec<Event>,
    /// Any state changes, such as setting KV
    pub state: Vec<StateChange>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkStatus {
    Start,
    Middle,
    End(Option<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventStatus {
    Pending,
    Running,
    /// The return value of the execution
    Completed(Option<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum StateChange {
    KVSet { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId {
    proc: ProcessId,
    seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProcessId {
    app: String,
    proc: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// This event's ID
    pub id: EventId,
    /// The event ID which caused this event
    pub cause: Option<EventId>,
    /// Arguments passed to the event. This is fed into the first chunk as
    /// a ChunkInput with itype="args" and value=args. Serialized as a JSON
    /// array.
    pub args: Option<String>,
    /// The status of a given event.
    pub status: EventStatus,
    /// Any optional metadata
    pub metadata: Option<String>,
}

/// Chunk inputs, used for replaying any indeterminism
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkInput {
    pub itype: String,
    pub value: String,
}
