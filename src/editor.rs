use std::sync::Arc;
use tokio::net::UnixStream;
use tokio::io::{BufReader, AsyncBufReadExt};
use serde_json::{json, Value};
use anyhow::Result;

use crate::config::Config;
use crate::workspace;
use crate::multiplexer::{
    Multiplexer, ServerInstance, ClientSession, ClientId,
    read_lsp_message, write_lsp_message,
};

#[derive(serde::Deserialize, serde::Serialize)]
struct ShimHandshake {
    language: String,
    cwd: String,
}

pub async fn handle_editor_connection(
    stream: UnixStream,
    multiplexer: Arc<Multiplexer>,
    config: Config,
) -> Result<()> {
    let mut reader = BufReader::new(stream);
    
    // 1. Read shim metadata handshake (first line of connection)
    let mut handshake_line = String::new();
    reader.read_line(&mut handshake_line).await?;
    let handshake: ShimHandshake = serde_json::from_str(handshake_line.trim())?;
    
    // Resolve project root and language server command
    let project_root = workspace::find_project_root(&handshake.cwd);
    let server_cmd = config.servers.get(&handshake.language)
        .map(|s| s.command.clone())
        .unwrap_or_default();
    
    let server_instance = multiplexer.get_or_spawn_server(project_root, handshake.language, &server_cmd).await?;
    let client_id = multiplexer.get_client_id().await;
    
    // Create channel for writing back to this editor client
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    
    let stream_writer = reader.into_inner();
    let (socket_read, mut socket_write) = stream_writer.into_split();
    
    // Spawn task to write messages from channel back to editor socket
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = write_lsp_message(&mut socket_write, &msg).await {
                eprintln!("Error writing to editor client: {:?}", e);
                break;
            }
        }
    });
    
    // Register client session
    {
        let mut server = server_instance.lock().await;
        server.clients.insert(client_id, ClientSession {
            id: client_id,
            tx: tx.clone(),
            open_files: Vec::new(),
            is_fleeting: false,
        });
    }
    
    // 2. Client loop reading standard LSP messages
    let mut socket_reader = BufReader::new(socket_read);
    loop {
        match read_lsp_message(&mut socket_reader).await {
            Ok(msg) => {
                let mut server = server_instance.lock().await;
                server.last_activity = std::time::Instant::now();
                if let Err(e) = process_editor_message(&mut *server, client_id, &msg).await {
                    eprintln!("Error processing editor message: {:?}", e);
                }
            }
            Err(_) => {
                // Client disconnected
                break;
            }
        }
    }
    
    // Unregister and cleanup open documents reference counts
    {
        let mut server = server_instance.lock().await;
        if let Some(session) = server.clients.remove(&client_id) {
            for file_uri in session.open_files {
                if let Some(count) = server.open_documents.get_mut(&file_uri) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        server.open_documents.remove(&file_uri);
                        // Forward didClose to LSP server
                        let close_notif = json!({
                            "jsonrpc": "2.0",
                            "method": "textDocument/didClose",
                            "params": {
                                "textDocument": { "uri": file_uri }
                            }
                        });
                        let body = serde_json::to_string(&close_notif)?;
                        let _ = write_lsp_message(&mut server.stdin, &body).await;
                    }
                }
            }
        }
        server.last_activity = std::time::Instant::now();
    }
    
    Ok(())
}

async fn process_editor_message(
    server: &mut ServerInstance,
    client_id: ClientId,
    msg: &str,
) -> Result<()> {
    let mut json: Value = serde_json::from_str(msg)?;
    let method = json.get("method").and_then(|m| m.as_str()).unwrap_or("");
    
    // Handle initialize locally/caching
    if method == "initialize" {
        if let Some(cached) = &server.cached_capabilities {
            if let Some(client) = server.clients.get(&client_id) {
                // Synthesize Response
                let response = json!({
                    "jsonrpc": "2.0",
                    "id": json.get("id").unwrap_or(&json!(1)),
                    "result": cached
                });
                let _ = client.tx.send(serde_json::to_string(&response)?);
            }
            return Ok(());
        }
        
        // Handshake not cached yet, forward it
        let client_req_id = json.get("id").cloned().unwrap_or(json!(1));
        let server_id = server.next_server_id;
        server.next_server_id += 1;
        server.id_mapping.insert(server_id, (client_id, client_req_id));
        
        json["id"] = json!(server_id);
        
        let body = serde_json::to_string(&json)?;
        write_lsp_message(&mut server.stdin, &body).await?;
        return Ok(());
    }
    
    if method == "initialized" {
        if !server.initialized {
            server.initialized = true;
            write_lsp_message(&mut server.stdin, msg).await?;
        }
        return Ok(());
    }
    
    if method == "textDocument/didOpen" {
        if let Some(uri) = json.get("params").and_then(|p| p.get("textDocument")).and_then(|t| t.get("uri")).and_then(|u| u.as_str()) {
            let count = server.open_documents.entry(uri.to_string()).or_insert(0);
            *count += 1;
            
            if let Some(client) = server.clients.get_mut(&client_id) {
                client.open_files.push(uri.to_string());
            }
            
            if *count == 1 {
                // Forward only the first open
                write_lsp_message(&mut server.stdin, msg).await?;
            }
        }
        return Ok(());
    }
    
    if method == "textDocument/didClose" {
        if let Some(uri) = json.get("params").and_then(|p| p.get("textDocument")).and_then(|t| t.get("uri")).and_then(|u| u.as_str()) {
            if let Some(count) = server.open_documents.get_mut(uri) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    server.open_documents.remove(uri);
                    write_lsp_message(&mut server.stdin, msg).await?;
                }
            }
            if let Some(client) = server.clients.get_mut(&client_id) {
                client.open_files.retain(|f| f != uri);
            }
        }
        return Ok(());
    }
    
    let has_id = json.get("id").is_some();
    let has_method = json.get("method").is_some();
    
    if has_id && has_method {
        // Route standard request with ID rewriting
        let server_id = server.next_server_id;
        server.next_server_id += 1;
        if let Some(id_val) = json.get("id") {
            server.id_mapping.insert(server_id, (client_id, id_val.clone()));
        }
        
        json["id"] = json!(server_id);
        let body = serde_json::to_string(&json)?;
        write_lsp_message(&mut server.stdin, &body).await?;
    } else {
        // Forward generic notifications OR responses to server-initiated requests directly
        write_lsp_message(&mut server.stdin, msg).await?;
    }
    
    Ok(())
}
