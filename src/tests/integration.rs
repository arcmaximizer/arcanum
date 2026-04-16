use std::io::Write;

use workspace::log::Event;
use workspace::executor::InMemoryKVState;
use deno_core::{FsModuleLoader, JsRuntime, RuntimeOptions, ModuleSpecifier};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use tempfile::NamedTempFile;

pub struct TestRuntime {
    js_runtime: JsRuntime,
    kv: Arc<TokioMutex<InMemoryKVState>>,
    effects: Arc<TokioMutex<Vec<Event>>>,
}

impl TestRuntime {
    pub fn new() -> Self {
        let kv: Arc<TokioMutex<InMemoryKVState>> = Arc::new(TokioMutex::new(InMemoryKVState::default()));
        let effects: Arc<TokioMutex<Vec<Event>>> = Arc::new(TokioMutex::new(Vec::new()));
        
        let js_runtime = JsRuntime::new(RuntimeOptions {
            extensions: vec![workspace::ops::executor_ops::init()],
            module_loader: Some(Rc::new(FsModuleLoader)),
            ..Default::default()
        });
        
        let op_state = js_runtime.op_state();
        let mut runtime = op_state.borrow_mut();
        runtime.put(Arc::clone(&kv));
        runtime.put(Arc::clone(&effects));
        
        Self {
            js_runtime,
            kv,
            effects,
        }
    }
    
    /// Run a complete handler scenario in one go
    pub async fn run_handler(&mut self, code: &str) -> Result<(), anyhow::Error> {
        self.run_js(code).await
    }
    
    pub async fn run_js(&mut self, code: &str) -> Result<(), anyhow::Error> {
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write_all(code.as_bytes())?;
        let path = temp_file.path();
        
        let module_specifier = ModuleSpecifier::from_file_path(path).unwrap();
        
        let main_module = self.js_runtime.load_main_es_module(&module_specifier).await?;
        let result = self.js_runtime.mod_evaluate(main_module);
        self.js_runtime.run_event_loop(Default::default()).await?;
        result.await?;
        Ok(())
    }
    
    pub async fn get_kv(&self, process: &str, key: &str) -> Option<String> {
        let kv = self.kv.lock().await;
        kv.get(process, key)
    }
    
    pub async fn get_effects(&self) -> Vec<Event> {
        self.effects.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_process_handler_kv() {
        let mut runtime = TestRuntime::new();
        
        // Define and run handler in one script - simulates loading handler then calling it
        let code = r#"
            // Handler definition (would be loaded once)
            async function handler(ctx, msg) {
                if (msg.type === "get") {
                    return { name: await ctx.kv.get("name"), bio: await ctx.kv.get("bio") };
                } else if (msg.type === "setName") {
                    await ctx.kv.set("name", msg.name);
                } else if (msg.type === "setBio") {
                    await ctx.kv.set("bio", msg.bio);
                }
            }
            
            // Context object - what gets passed to handler
            const ctx = {
                kv: {
                    get: (key) => Deno.core.ops.op_kv_get("infoboard", key),
                    set: (k, v) => Deno.core.ops.op_kv_set("infoboard", k, v),
                },
                send: (target, m) => Deno.core.ops.op_send(target, m),
            };
            
            // Simulate multiple calls (what you'd do across requests)
            (async () => {
                await handler(ctx, {type: "setName", name: "Alice"});
                await handler(ctx, {type: "setBio", bio: "Developer"});
                await handler(ctx, {type: "get"});
            })();
        "#;
        
        runtime.run_handler(code).await.unwrap();
        
        // State persisted!
        assert_eq!(runtime.get_kv("infoboard", "name").await, Some("Alice".to_string()));
        assert_eq!(runtime.get_kv("infoboard", "bio").await, Some("Developer".to_string()));
    }
    
    #[tokio::test]
    async fn test_process_handler_send() {
        let mut runtime = TestRuntime::new();
        
        let code = r#"
            async function handler(ctx, msg) {
                if (msg.type === "forward") {
                    await ctx.send(msg.target, msg.content);
                }
            }
            
            const ctx = {
                kv: { get: () => null, set: () => {} },
                send: (target, m) => Deno.core.ops.op_send(target, m),
            };
            
            (async () => {
                await handler(ctx, {type: "forward", target: "otherapp/otherproc", content: "hi"});
            })();
        "#;
        
        runtime.run_handler(code).await.unwrap();
        
        let effects = runtime.get_effects().await;
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].id.proc.proc, "otherproc");
        assert_eq!(effects[0].id.proc.app, "otherapp");
    }
}