# Developer Guide: Building Clients against `lsp-broker`

This guide explains how to connect and build clients against the `lsp-broker` daemon.

---

## 1. Multiplexed Persistent Editor Integration (`lsp-broker.sock`)

For long-lived editor sessions (like code editors, IDE plugins), use the `lsp-broker.sock` Unix domain socket.

### Connection Protocol:

1. **Establish Connection**: Open a TCP/Unix connection to `lsp-broker.sock`.
2. **Metadata Handshake**: Before sending any LSP payloads, the client **must** send a single JSON line specifying the target programming language and working directory.
   ```json
   {"language": "rust", "cwd": "/home/user/my_project"}
   ```
3. **LSP JSON-RPC Framing**:
   * Once the handshake is sent, follow standard LSP JSON-RPC framing (prefixed with `Content-Length: ...\r\n\r\n`).
   * Send the standard `initialize` request first. `lsp-broker` will handle caching capabilities.
   * Send `textDocument/didOpen`, `textDocument/didChange`, `textDocument/didClose` to keep document buffers in sync.
   * The broker automatically ref-counts open files across multiple editor clients and rewrites Request IDs as needed.

---

## 2. Fleeting Query Integration (`lsp-broker-query.sock`)

For stateless query tools (like visual popups, CLI reference finders, `Purse`, `gluek-up`), use `lsp-broker-query.sock`.

### Connection Protocol:

1. **Establish Connection**: Open a TCP/Unix connection to `lsp-broker-query.sock`.
2. **Send Query Directly**:
   * No handshake or standard `initialize` / `didOpen` is needed from the client!
   * Send the target LSP request directly (e.g. `textDocument/references` or `textDocument/definition`) with standard `Content-Length` headers.
   * Example query payload:
     ```json
     {
       "jsonrpc": "2.0",
       "id": 1,
       "method": "textDocument/references",
       "params": {
         "textDocument": { "uri": "file:///path/to/file.md" },
         "position": { "line": 5, "character": 12 },
         "context": { "includeDeclaration": true }
       }
     }
     ```
3. **Execution flow**:
   * The broker will resolve the workspace root and spawn the LSP server if it isn't running.
   * The broker will automatically read the latest file content from disk, send a simulated `didOpen` to refresh the server's VFS cache, run your query, send `didClose`, and return the response.
   * Read the response and close the connection.
