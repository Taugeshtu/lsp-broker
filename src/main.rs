use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, Duration};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio::io::{BufReader, AsyncBufReadExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use anyhow::{anyhow, Result};

mod config;
mod workspace;
mod multiplexer;

use config::Config;
use multiplexer::{Multiplexer, ServerInstance, ClientSession, ClientId, read_lsp_message, write_lsp_message};

#[derive(Debug, Deserialize, Serialize)]
struct ShimHandshake {
    language: String,
    cwd: String,
}

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
                    if let Err(e) = handle_editor_connection(stream, mult, conf).await {
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
                    if let Err(e) = handle_query_connection(stream, mult, conf).await {
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

async fn handle_editor_connection(
    mut stream: UnixStream,
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
    
    let mut stream_writer = reader.into_inner();
    let (mut socket_read, mut socket_write) = stream_writer.into_split();
    
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
        
        // Intercept response to cache it (handled in stdout reading loop by detecting initialize response)
        // Wait, standard stdout loop handles all replies. We must detect when it's the initialize reply.
        // Let's flag the ID mapping so handle_server_message knows to cache the result!
        // We do this by mapping the server_id to a special client session or keeping a flag.
        // We'll rewrite the stdout handler to detect when the response is for "initialize" and cache it.
        // Actually, we can check if the method in client request was "initialize" from the mapping!
        // Let's rewrite id_mapping to: server_id -> (client_id, client_req_id, is_initialize)
        // Wait, in multiplexer.rs, handle_server_message retrieves the mapping. We can adjust the stdout reader
        // to detect if the response contains "capabilities" and cache it.
        // Even simpler: the first response received with capabilities is cached as cached_capabilities.
        
        let body = serde_json::to_string(&json)?;
        write_lsp_message(&mut server.stdin, &body).await?;
        return Ok(());
    }
    
    if method == "initialized" {
        // Drop initialized notification to avoid double initializing
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
    
    // Route standard request with ID rewriting
    if let Some(id_val) = json.get("id") {
        let server_id = server.next_server_id;
        server.next_server_id += 1;
        server.id_mapping.insert(server_id, (client_id, id_val.clone()));
        
        json["id"] = json!(server_id);
        let body = serde_json::to_string(&json)?;
        write_lsp_message(&mut server.stdin, &body).await?;
    } else {
        // Forward generic notifications
        write_lsp_message(&mut server.stdin, msg).await?;
    }
    
    Ok(())
}

fn percent_decode(s: &str) -> String {
    let mut bytes = Vec::new();
    let mut bytes_iter = s.as_bytes().iter();
    while let Some(&b) = bytes_iter.next() {
        if b == b'%' {
            let h1 = bytes_iter.next().copied();
            let h2 = bytes_iter.next().copied();
            if let (Some(h1), Some(h2)) = (h1, h2) {
                if let Ok(val) = u8::from_str_radix(std::str::from_utf8(&[h1, h2]).unwrap_or("00"), 16) {
                    bytes.push(val);
                    continue;
                }
            }
            bytes.push(b'%');
            if let Some(x) = h1 { bytes.push(x); }
            if let Some(x) = h2 { bytes.push(x); }
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|_| s.to_string())
}

async fn handle_query_connection(
    stream: UnixStream,
    multiplexer: Arc<Multiplexer>,
    config: Config,
) -> Result<()> {
    println!("[lsp-broker] Fleeting query connection received.");
    let mut reader = BufReader::new(stream);
    
    // 1. Read the fleeting query message
    let query_msg = read_lsp_message(&mut reader).await?;
    let mut json: Value = serde_json::from_str(&query_msg)?;
    
    // Extract URI from query parameters
    let uri = json.get("params")
        .and_then(|p| p.get("textDocument"))
        .and_then(|t| t.get("uri"))
        .and_then(|u| u.as_str())
        .ok_or_else(|| anyhow!("Query missing textDocument.uri"))?;
    
    let raw_path = uri.trim_start_matches("file://");
    let decoded_path = percent_decode(raw_path);
    let path_str = &decoded_path;
    let project_root = workspace::find_project_root(path_str);
    
    // Detect language from extension or shebang
    let path = Path::new(path_str);
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let language = config.languages.get(ext).cloned()
        .or_else(|| workspace::detect_shebang_language(path_str))
        .ok_or_else(|| anyhow!("Could not detect language for {:?}", path_str))?;
    
    println!("[lsp-broker] File: '{}' | Language: '{}' | Project Root: {:?}", path_str, language, project_root);
    
    let server_cmd = config.servers.get(&language)
        .map(|s| s.command.clone())
        .unwrap_or_default();
    
    println!("[lsp-broker] Targeting server command: {:?}", server_cmd);
    
    // Log currently active servers
    {
        let servers = multiplexer.servers.lock().await;
        println!("[lsp-broker] Active Servers count: {}", servers.len());
        for ((root, lang), inst_mutex) in servers.iter() {
            let inst = inst_mutex.lock().await;
            println!("  * Server: '{}' -> Root: {:?} [Clients: {}]", lang, root, inst.clients.len());
        }
    }
    
    let server_instance = multiplexer.get_or_spawn_server(project_root, language, &server_cmd).await?;
    
    // 2. Perform on-demand initialization if necessary
    {
        let mut server = server_instance.lock().await;
        if server.cached_capabilities.is_none() {
            // Synthesize standard initialization handshake
            let init_req = json!({
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {
                    "processId": null,
                    "rootUri": format!("file://{}", server.project_root.display()),
                    "capabilities": {
                        "textDocument": {
                            "definition": { "dynamicRegistration": true },
                            "references": { "dynamicRegistration": true }
                        }
                    }
                }
            });
            
            // Register ID 0 in the mapping so the stdout reader parses and caches it
            server.id_mapping.insert(0, (0, json!(0)));
            
            let body = serde_json::to_string(&init_req)?;
            write_lsp_message(&mut server.stdin, &body).await?;
            
            // Read response from server. Since we are inside the client connection handler, we should await
            // the server to initialize. The stdout reader task will process stdout and cache it.
            // We can just poll for up to 30 seconds until cached_capabilities is populated!
            let start = Instant::now();
            while server.cached_capabilities.is_none() {
                if start.elapsed() > Duration::from_secs(30) {
                    return Err(anyhow!("LSP server initialization timed out"));
                }
                drop(server);
                tokio::time::sleep(Duration::from_millis(50)).await;
                server = server_instance.lock().await;
            }
            
            // Send initialized notification to server
            let initialized_notif = json!({
                "jsonrpc": "2.0",
                "method": "initialized",
                "params": {}
            });
            let body = serde_json::to_string(&initialized_notif)?;
            write_lsp_message(&mut server.stdin, &body).await?;
        }
    }
    
    // 3. Dispatch the query
    let client_id = multiplexer.get_client_id().await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    
    // Temporarily register this client to receive the response
    {
        let mut server = server_instance.lock().await;
        server.clients.insert(client_id, ClientSession {
            id: client_id,
            tx,
            open_files: Vec::new(),
            is_fleeting: true,
        });
        
        let client_req_id = json.get("id").cloned().unwrap_or(json!(1));
        let server_id = server.next_server_id;
        server.next_server_id += 1;
        server.id_mapping.insert(server_id, (client_id, client_req_id));
        
        json["id"] = json!(server_id);
        let body = serde_json::to_string(&json)?;
        write_lsp_message(&mut server.stdin, &body).await?;
    }
    
    // 4. Await response and write back to query socket
    let mut socket_writer = reader.into_inner();
    let response = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    
    // Cleanup temporary client registration
    {
        let mut server = server_instance.lock().await;
        server.clients.remove(&client_id);
        server.last_activity = Instant::now();
    }
    
    if let Ok(Some(resp_msg)) = response {
        write_lsp_message(&mut socket_writer, &resp_msg).await?;
    } else {
        return Err(anyhow!("LSP server query timed out"));
    }
    
    Ok(())
}
