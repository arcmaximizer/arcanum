use workspace::log::Event;
use workspace::executor::{InMemoryKVState, create_runtime, load_handler, call_handler, LoadedHandler};
use deno_core::JsRuntime;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

pub struct TestRuntime {
    js_runtime: JsRuntime,
    handler: LoadedHandler,
    kv: Arc<TokioMutex<InMemoryKVState>>,
    effects: Arc<TokioMutex<Vec<Event>>>,
}

impl TestRuntime {
    pub fn new() -> Self {
        let kv: Arc<TokioMutex<InMemoryKVState>> = Arc::new(TokioMutex::new(InMemoryKVState::default()));
        let effects: Arc<TokioMutex<Vec<Event>>> = Arc::new(TokioMutex::new(Vec::new()));
        
        let js_runtime = create_runtime(
            Arc::clone(&kv),
            Arc::clone(&effects),
        );
        
        Self {
            js_runtime,
            handler: LoadedHandler { module_id: 0 },
            kv,
            effects,
        }
    }
    
    /// Load handler code (the exported handler function)
    pub async fn load(&mut self, code: &str) -> Result<(), anyhow::Error> {
        self.handler = load_handler(&mut self.js_runtime, code).await?;
        Ok(())
    }
    
    /// Call the loaded handler with a message
    pub async fn call(&mut self, msg: serde_json::Value) -> Result<serde_json::Value, anyhow::Error> {
        call_handler(&mut self.js_runtime, msg).await
    }
    
    pub async fn get_kv(&self, process: &str, key: &str) -> Option<String> {
        let kv = self.kv.lock().await;
        kv.get(process, key)
    }
    
    pub async fn get_effects(&self) -> Vec<Event> {
        self.effects.lock().await.clone()
    }
}

const HANDLER_CODE: &str = r#"
const ctx = {
    kv: {
        get: (key) => Deno.core.ops.op_kv_get("infoboard", key),
        set: (k, v) => Deno.core.ops.op_kv_set("infoboard", k, v),
    },
    send: (target, m) => Deno.core.ops.op_send(target, m),
};

async function handler(msg) {
    if (msg.type === "get") {
        return { name: await ctx.kv.get("name"), bio: await ctx.kv.get("bio") };
    } else if (msg.type === "setName") {
        await ctx.kv.set("name", msg.name);
    } else if (msg.type === "setBio") {
        await ctx.kv.set("bio", msg.bio);
    } else if (msg.type === "forward") {
        await ctx.send(msg.target, msg.content);
    }
}

globalThis.handler = handler;
"#;

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_process_handler_kv() {
        let mut runtime = TestRuntime::new();
        
        runtime.load(HANDLER_CODE).await.unwrap();
        
        runtime.call(serde_json::json!({"type": "setName", "name": "Alice"})).await.unwrap();
        runtime.call(serde_json::json!({"type": "setBio", "bio": "Developer"})).await.unwrap();
        let result = runtime.call(serde_json::json!({"type": "get"})).await.unwrap();
        
        assert_eq!(runtime.get_kv("infoboard", "name").await, Some("Alice".to_string()));
        assert_eq!(runtime.get_kv("infoboard", "bio").await, Some("Developer".to_string()));
        println!("call result: {:?}", result);
        println!("kv name: {:?}", runtime.get_kv("infoboard", "name").await);
        println!("kv bio: {:?}", runtime.get_kv("infoboard", "bio").await);
    }
    
    #[tokio::test]
    async fn test_process_handler_send() {
        let mut runtime = TestRuntime::new();
        
        runtime.load(HANDLER_CODE).await.unwrap();
        
        let result = runtime.call(serde_json::json!({
            "type": "forward",
            "target": "otherapp/otherproc",
            "content": "hi"
        })).await.unwrap();
        
        let effects = runtime.get_effects().await;
        println!("effects: {:?}", effects);
        println!("call result: {:?}", result);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].id.proc.proc, "otherproc");
        assert_eq!(effects[0].id.proc.app, "otherapp");
        assert_eq!(effects[0].args, Some(r#"["hi"]"#.to_string()));
    }
}