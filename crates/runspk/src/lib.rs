pub mod api;
pub mod backend;
pub mod client;
pub mod config;
pub mod hardware;
pub mod journal;
pub mod model;
pub mod policy;
pub mod profile;
pub mod runner;
mod staging;
mod state_lock;
pub mod store;
pub mod telemetry;
pub mod worker;
pub mod workspace;

pub fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
}

pub async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
}
