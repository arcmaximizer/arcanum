use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct EventId {
    pub namespace: String,
    pub app: String,
    pub proc: String,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ProcessId {
    pub namespace: String,
    pub app: String,
    pub proc: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct HandlerId {
    pub namespace: String,
    pub app: String,
    pub handler: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct AppId {
    pub namespace: String,
    pub app: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub input: String,
    pub reason: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to parse '{}': {}", self.input, self.reason)
    }
}

impl std::error::Error for ParseError {}

impl TryFrom<&str> for ProcessId {
    type Error = ParseError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let stripped = s.strip_prefix('^').unwrap_or(s);
        let parts: Vec<&str> = stripped.splitn(3, '/').collect();
        match parts.len() {
            3 => {
                if parts.iter().any(|p| p.is_empty()) {
                    return Err(ParseError {
                        input: s.to_string(),
                        reason: "ProcessId parts must not be empty".to_string(),
                    });
                }
                Ok(ProcessId {
                    namespace: parts[0].to_string(),
                    app: parts[1].to_string(),
                    proc: parts[2].to_string(),
                })
            }
            2 => {
                if parts[0].is_empty() || parts[1].is_empty() {
                    return Err(ParseError {
                        input: s.to_string(),
                        reason: "ProcessId parts must not be empty".to_string(),
                    });
                }
                Ok(ProcessId {
                    namespace: parts[0].to_string(),
                    app: parts[1].to_string(),
                    proc: "entrypoint".to_string(),
                })
            }
            _ => Err(ParseError {
                input: s.to_string(),
                reason:
                    "ProcessId requires two or three parts (namespace/app or namespace/app/process)"
                        .to_string(),
            }),
        }
    }
}

impl TryFrom<&str> for AppId {
    type Error = ParseError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let stripped = s.strip_prefix('^').unwrap_or(s);
        let parts: Vec<&str> = stripped.splitn(2, '/').collect();
        if parts.len() != 2 || parts.iter().any(|p| p.is_empty()) {
            return Err(ParseError {
                input: s.to_string(),
                reason: "AppId requires exactly one slash (namespace/app)".to_string(),
            });
        }
        Ok(AppId {
            namespace: parts[0].to_string(),
            app: parts[1].to_string(),
        })
    }
}

impl From<&HandlerId> for AppId {
    fn from(val: &HandlerId) -> AppId {
        AppId {
            namespace: val.namespace.clone(),
            app: val.app.clone(),
        }
    }
}

impl From<ProcessId> for AppId {
    fn from(val: ProcessId) -> AppId {
        AppId {
            namespace: val.namespace,
            app: val.app,
        }
    }
}

impl From<&ProcessId> for AppId {
    fn from(val: &ProcessId) -> AppId {
        AppId {
            namespace: val.namespace.clone(),
            app: val.app.clone(),
        }
    }
}

impl AppId {
    pub fn with_process(&self, proc: String) -> ProcessId {
        ProcessId {
            namespace: self.namespace.clone(),
            app: self.app.clone(),
            proc,
        }
    }
}

impl From<ProcessId> for String {
    fn from(val: ProcessId) -> String {
        val.to_string()
    }
}

impl From<AppId> for String {
    fn from(val: AppId) -> String {
        val.to_string()
    }
}

impl From<EventId> for String {
    fn from(val: EventId) -> String {
        val.to_string()
    }
}

impl From<HandlerId> for String {
    fn from(val: HandlerId) -> String {
        val.to_string()
    }
}

impl fmt::Display for ProcessId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "^{}/{}/{}", self.namespace, self.app, self.proc)
    }
}

impl fmt::Display for HandlerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "^{}/{}/{}", self.namespace, self.app, self.handler)
    }
}

impl fmt::Display for AppId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "^{}/{}", self.namespace, self.app)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "^{}/{}/{}/e{}",
            self.namespace, self.app, self.proc, self.seq
        )
    }
}
