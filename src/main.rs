use alloy::api;
use alloy::app_state::AppState;
use alloy::error::AppError;
use alloy::persistence::MemoryStore;
use alloy::session::TicketIssuer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    setup_tracing();

    let addr: SocketAddr = "0.0.0.0:3000".parse()?;
    let listener = TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;

    let store = Arc::new(MemoryStore::default());
    let ticketing = TicketIssuer::from_env_or_generate();
    let state = Arc::new(AppState::with_components(store, ticketing));

    let app = api::router(state);

    info!(
        "starting yrs-axum server on {}; websocket endpoint /edit",
        bound_addr
    );
    info!("logging is configured and working - connect a client to see document logs");

    axum::serve(listener, app).await?;

    Ok(())
}

fn setup_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "debug,alloy=trace,axum::rejection=trace".into());

    eprintln!("Tracing filter: {}", filter);

    tracing_subscriber::fmt().with_env_filter(filter).init();
}
