# axetui

A simple Rust + Ratatui dashboard for a Bitaxe miner.

It connects to a live Bitaxe over HTTP and WebSocket, then displays:

- hashrate history
- power / heat / fan meters
- current Bitaxe status
- live log output from `/api/ws`

## Requirements

- Rust toolchain
- a reachable Bitaxe running AxeOS

## Run

Use the default local hostname:

```bash
cargo run -- http://bitaxe.local
```

Or point it at a specific miner IP:

```bash
cargo run -- http://192.168.1.19
```

You can also use the `BITAXE_URL` environment variable:

```bash
BITAXE_URL=http://192.168.1.19 cargo run
```

## Controls

- `q`: quit
- `r`: refresh immediately

## Build

```bash
cargo build
```
