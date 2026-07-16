use std::path::Path;
use std::sync::Arc;
use tokio::net::UnixListener;
use anyhow::Result;

mod config;
mod workspace;
mod multiplexer;
mod editor;
mod query;

use config::Config;
use multiplexer::Multiplexer;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load();
    let multiplexer = Arc::new(Multiplexer::new());
    
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .unwrap_or_else(|_| "/run/user/1000".to_string());
    
    // Sockets paths
    let editor_socket_path = Path::new(&runtime_dir).join("lsp-broker.sock");
    let query_socket_path = Path::new(&runtime_dir).join("lsp-broker-query.sock");
    
    // Clean up old sockets
    let _ = std::fs::remove_file(&editor_socket_path);
    let _ = std::fs::remove_file(&query_socket_path);
    
    // Bind listeners
    let editor_listener = UnixListener::bind(&editor_socket_path)?;
    let query_listener = UnixListener::bind(&query_socket_path)?;
    
    println!("lsp-broker Editor socket: {:?}", editor_socket_path);
    println!("lsp-broker Query socket: {:?}", query_socket_path);
    
    let multiplexer_editor = multiplexer.clone();
    let config_editor = config.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = editor_listener.accept().await {
                let mult = multiplexer_editor.clone();
                let conf = config_editor.clone();
                tokio::spawn(async move {
                    if let Err(e) = editor::handle_editor_connection(stream, mult, conf).await {
                        eprintln!("Editor connection error: {:?}", e);
                    }
                });
            }
        }
    });
    
    let multiplexer_query = multiplexer.clone();
    let config_query = config.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = query_listener.accept().await {
                let mult = multiplexer_query.clone();
                let conf = config_query.clone();
                tokio::spawn(async move {
                    if let Err(e) = query::handle_query_connection(stream, mult, conf).await {
                        eprintln!("Query connection error: {:?}", e);
                    }
                });
            }
        }
    });
    
    // Keep main thread alive
    tokio::signal::ctrl_c().await?;
    println!("Shutting down lsp-broker...");
    
    Ok(())
}
