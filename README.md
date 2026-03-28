# ddlastgraph

Minimal Rust project scaffold created by Copilot.

Build:

```bash
cargo build
```

Run:

```bash
cargo run
```

Parsing SQL DDL

You can parse a SQL file (DDL) with the added `sqlparser`-based tool:

```bash
cd /Users/tthogho1/Documents/source/ddlastgraph
cargo run -- sample.sql
```

Or pipe SQL via stdin:

```bash
cat sample.sql | cargo run
```
