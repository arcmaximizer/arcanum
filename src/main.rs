mod executor;
mod log;
mod state;
mod store;

fn main() {
    println!("Arcanum isn't ready yet.");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Err(error) = runtime.block_on(executor::run_js("./example.js")) {
        eprintln!("error: {}", error);
    }
}
