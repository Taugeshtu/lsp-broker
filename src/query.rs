use std::sync::Arc;
use std::path::Path;
use std::time::{Instant, Duration};
use tokio::net::UnixStream;
use tokio::io::BufReader;
use serde_json::{json, Value};
use anyhow::{anyhow, Result};

use crate::config::Config;
use crate::workspace;
use crate::multiplexer::{
    Multiplexer, ClientSession, read_lsp_message, write_lsp_message,
};

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

pub async fn handle_query_connection(
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
        .map(|u| u.to_string())
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
        let workspaces = multiplexer.workspaces.lock().await;
        let mut total_servers = 0;
        for workspace_mutex in workspaces.values() {
            let workspace = workspace_mutex.lock().await;
            total_servers += workspace.servers.len();
        }
        println!("[lsp-broker] Active Servers count: {}", total_servers);
        for (root, workspace_mutex) in workspaces.iter() {
            let workspace = workspace_mutex.lock().await;
            for (lang, inst_mutex) in workspace.servers.iter() {
                let inst = inst_mutex.lock().await;
                println!("  * Server: '{}' -> Root: {:?} [Clients: {}]", lang, root, inst.clients.len());
            }
        }
    }
    
    let server_instance = multiplexer.get_or_spawn_server(project_root, language.clone(), &server_cmd).await?;
    
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
            server.initialized = true;
        }
    }
    
    // 3. Dispatch the query
    let client_id = multiplexer.get_client_id().await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    
    let mut file_content = None;
    
    // Temporarily register this client to receive the response
    {
        let mut server = server_instance.lock().await;
        
        let is_already_open = server.open_documents.contains_key(&uri);
        if !is_already_open {
            if let Ok(content) = std::fs::read_to_string(&decoded_path) {
                file_content = Some(content.clone());
                let did_open = json!({
                    "jsonrpc": "2.0",
                    "method": "textDocument/didOpen",
                    "params": {
                        "textDocument": {
                            "uri": uri.clone(),
                            "languageId": language,
                            "version": 1,
                            "text": content
                        }
                    }
                });
                let _ = write_lsp_message(&mut server.stdin, &serde_json::to_string(&did_open)?).await;
            }
        }
    }
    
    // Sleep briefly to avoid race condition where server has not finished parsing didOpen
    // before it receives the references query.
    if file_content.is_some() {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    
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
        
        if file_content.is_some() {
            let did_close = json!({
                "jsonrpc": "2.0",
                "method": "textDocument/didClose",
                "params": {
                    "textDocument": {
                        "uri": uri.clone()
                    }
                }
            });
            let _ = write_lsp_message(&mut server.stdin, &serde_json::to_string(&did_close)?).await;
        }
    }
    
    if let Ok(Some(resp_msg)) = response {
        write_lsp_message(&mut socket_writer, &resp_msg).await?;
    } else {
        return Err(anyhow!("LSP server query timed out"));
    }
    
    Ok(())
}
