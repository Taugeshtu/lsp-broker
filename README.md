> "Because running five `rust-analyzer` processes on the same project is a crime against RAM."

## why

My workflow has changed from "let's open one massive do-it-all IDE" to "let's open 5 separate text editor windows", and LSP servers are not cheap. `lsp-broker` enforces that exactly one server process runs per `(project, language)` pair system-wide, sharing it multiplexed across all client connections. It also exposes a stateless query interface for fleeting tools.

## how

`lsp-broker` runs as a background daemon listening on two Unix domain sockets:
1. `lsp-broker.sock`: Multiplexes persisting editors (uses standard LSP JSON-RPC framing, rewrites request IDs, ref-counts document opens).
2. `lsp-broker-query.sock`: Serves fleeting query clients (`gluek-up`, `Purse`) requesting quick answers without standard session handshakes.

It resolves workspace roots by climbing directories for `.git` or `.project-root` markers, maps languages by extension config, shebang-inspects scripts, and dynamically manages server lifecycles with a 15-minute idle-reaper.

## Install

Dependencies: 
- [rust installed in your system](https://rust-lang.org/tools/install/)

Build & install with cargo:
```bash
cargo install --git https://github.com/Taugeshtu/lsp-broker --root ~/.local
```

_Alternatively:_
```bash
# navigate to where you want it to live, for example, ~/Applications/Gits
git clone https://github.com/Taugeshtu/lsp-broker
cd lsp-broker
cargo install --path . --root ~/.local
```

This puts `lsp-broker` in `~/.local/bin/`.

## Configuration

Configurations live in `~/.config/lsp-broker/config.toml`. If missing, the broker falls back to default commands for Rust, Python, Go, and Markdown.

Example:
```toml
[languages]
rs = "rust"
md = "markdown"

[servers.rust]
command = ["rust-analyzer"]

[servers.markdown]
command = ["markdown-oxide"]
```

## Version history

future:
- [ ] more graceful server killing mechanism, taking into account system's memory pressure
- [ ] supporting multiple editors on the same file via faux "synchronization" of buffers by sending edits from one editor to others
- [ ] multiple lsp-servers per same kind of file?

v0.1.0:
- [x] initial release
