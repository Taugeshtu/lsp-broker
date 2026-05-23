use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::Mutex;
use serde_json::Value;
use anyhow::{anyhow, Result};

pub type ClientId = usize;

#[derive(Debug, Clone)]
pub struct ClientSession {
    pub id: ClientId,
    pub tx: tokio::sync::mpsc::UnboundedSender<String>,
    pub open_files: Vec<String>,
    pub is_fleeting: bool,
}

pub struct ServerInstance {
    pub project_root: PathBuf,
    pub language: String,
    pub child: Child,
    pub stdin: ChildStdin,
    pub clients: HashMap<ClientId, ClientSession>,
    pub open_documents: HashMap<String, usize>, // uri -> count
    pub id_mapping: HashMap<u64, (ClientId, Value)>, // server_id -> (client_id, client_req_id)
    pub next_server_id: u64,
    pub cached_capabilities: Option<Value>,
    pub last_activity: Instant,
}

pub struct Multiplexer {
    pub servers: Arc<Mutex<HashMap<(PathBuf, String), Arc<Mutex<ServerInstance>>>>>,
    pub next_client_id: Arc<Mutex<ClientId>>,
}

impl Multiplexer {
    pub fn new() -> Self {
        let servers: Arc<Mutex<HashMap<(PathBuf, String), Arc<Mutex<ServerInstance>>>>> = Arc::new(Mutex::new(HashMap::new()));
        
        // Spawn the idle server reaping task
        let servers_clone = servers.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let mut lock = servers_clone.lock().await;
                let mut to_remove = Vec::new();
                
                for (key, server_mutex) in lock.iter() {
                    let mut server = server_mutex.lock().await;
                    if server.clients.is_empty() && server.last_activity.elapsed() > Duration::from_secs(15 * 60) {
                        println!("Killing idle server for language '{}' at {:?}", key.1, key.0);
                        let _ = server.child.kill().await;
                        to_remove.push(key.clone());
                    }
                }
                
                for key in to_remove {
                    lock.remove(&key);
                }
            }
        });

        Multiplexer {
            servers,
            next_client_id: Arc::new(Mutex::new(1)),
        }
    }

    pub async fn get_or_spawn_server(
        &self,
        project_root: PathBuf,
        language: String,
        cmd_args: &[String],
    ) -> Result<Arc<Mutex<ServerInstance>>> {
        let mut servers = self.servers.lock().await;
        let key = (project_root.clone(), language.clone());

        if let Some(server) = servers.get(&key) {
            return Ok(server.clone());
        }

        if cmd_args.is_empty() {
            return Err(anyhow!("No command configured for language '{}'", language));
        }

        println!("Spawning LSP server for language '{}' in {:?}", language, project_root);
        let mut child = Command::new(&cmd_args[0])
            .args(&cmd_args[1..])
            .current_dir(&project_root)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()?;

        let stdin = child.stdin.take().ok_or_else(|| anyhow!("Failed to open stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| anyhow!("Failed to open stdout"))?;

        let server_instance = Arc::new(Mutex::new(ServerInstance {
            project_root: project_root.clone(),
            language: language.clone(),
            child,
            stdin,
            clients: HashMap::new(),
            open_documents: HashMap::new(),
            id_mapping: HashMap::new(),
            next_server_id: 1,
            cached_capabilities: None,
            last_activity: Instant::now(),
        }));

        // Spawn a task to read the server's stdout and route messages to clients
        let server_clone = server_instance.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                match read_lsp_message(&mut reader).await {
                    Ok(msg) => {
                        let mut server = server_clone.lock().await;
                        server.last_activity = Instant::now();
                        if let Err(e) = handle_server_message(&mut *server, &msg).await {
                            eprintln!("Error routing server message: {:?}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("LSP server stdout EOF or Error: {:?}", e);
                        break;
                    }
                }
            }
        });

        servers.insert(key, server_instance.clone());
        Ok(server_instance)
    }

    pub async fn get_client_id(&self) -> ClientId {
        let mut id = self.next_client_id.lock().await;
        let current = *id;
        *id += 1;
        current
    }
}

pub async fn read_lsp_message<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> Result<String> {
    let mut content_length = None;
    let mut line = String::new();
    
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            return Err(anyhow!("EOF"));
        }
        
        if line == "\r\n" || line == "\n" {
            break;
        }
        
        if line.to_lowercase().starts_with("content-length:") {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 2 {
                content_length = Some(parts[1].trim().parse::<usize>()?);
            }
        }
    }
    
    let length = content_length.ok_or_else(|| anyhow!("Missing Content-Length header"))?;
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).await?;
    
    Ok(String::from_utf8(body)?)
}

pub async fn write_lsp_message<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    body: &str,
) -> Result<()> {
    let payload = format!(
        "Content-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    writer.write_all(payload.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

async fn handle_server_message(server: &mut ServerInstance, msg: &str) -> Result<()> {
    let mut json: Value = serde_json::from_str(msg)?;
    if let Some(err) = json.get("error") {
        println!("[lsp-broker] Server error response: {:?}", err);
    }
    
    if let Some(id_val) = json.get("id") {
        // It's a response to a request
        if let Some(server_id) = id_val.as_u64() {
            if let Some((client_id, original_id)) = server.id_mapping.remove(&server_id) {
                // Cache capabilities when initialize response arrives
                if server.cached_capabilities.is_none() {
                    if let Some(result) = json.get("result") {
                        if result.get("capabilities").is_some() {
                            server.cached_capabilities = Some(result.clone());
                            println!("LSP server initialized. Cached capabilities.");
                        }
                    }
                }
                
                if let Some(client) = server.clients.get(&client_id) {
                    json["id"] = original_id;
                    let rewritten = serde_json::to_string(&json)?;
                    let _ = client.tx.send(rewritten);
                }
            }
        }
    } else {
        // It's a server-to-client notification (e.g. textDocument/publishDiagnostics)
        let method = json.get("method").and_then(|m| m.as_str()).unwrap_or("");
        if method == "textDocument/publishDiagnostics" {
            if let Some(uri) = json.get("params").and_then(|p| p.get("uri")).and_then(|u| u.as_str()) {
                // Route only to persistent editor clients that have this file open
                for client in server.clients.values() {
                    if !client.is_fleeting && client.open_files.iter().any(|f| f == uri) {
                        let _ = client.tx.send(msg.to_string());
                    }
                }
            }
        } else {
            // Broadcast standard notifications to all persistent editor clients only
            for client in server.clients.values() {
                if !client.is_fleeting {
                    let _ = client.tx.send(msg.to_string());
                }
            }
        }
    }
    
    Ok(())
}
