use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct EventId {
    pub app: String,
    pub proc: String,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ProcessId {
    pub app: String,
    pub proc: String,
}

impl From<ProcessId> for String {
    fn from(val: ProcessId) -> String {
        format!("{}/{}", val.app, val.proc)
    }
}

impl From<EventId> for String {
    fn from(val: EventId) -> String {
        format!("{}/{}/e{}", val.app, val.proc, val.seq)
    }
}

impl fmt::Display for ProcessId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.app, self.proc)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}/e{}", self.app, self.proc, self.seq)
    }
}
