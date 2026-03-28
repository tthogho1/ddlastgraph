# llm_parser

Parse a SQL AST dump and generate per-table English descriptions using an LLM.

Prerequisites

- Rust toolchain (cargo)
- An OpenAI API key

Using a `.env` file (recommended)

Create a `.env` file in the `llm_parser` folder:

```bash
echo 'OPENAI_API_KEY="sk-..."' > .env
# optionally specify model
echo 'OPENAI_MODEL="gpt-5-mini"' >> .env
```

The app loads `.env` automatically (via `dotenvy`).

Run

```bash
cd /Users/tthogho1/Documents/source/ddlastgraph/llm_parser
# provide path to the AST dump (e.g. ../user.txt)
cargo run -- ../user.txt
```

Behavior

- The tool parses the AST dump, groups statements by table, and sends one prompt per table to the LLM.
- Outputs are saved into `llm_outputs/<schema.table>.json` (one JSON per table).

Notes

- Do not commit your `.env` (add it to `.gitignore`).
- Each table is sent as a separate prompt to avoid token limits; this increases API calls and cost.
- The default model is `gpt-5-mini` but can be overridden with `OPENAI_MODEL`.

Example

```bash
# build once
cargo build
# run
cargo run -- ../user.txt
ls llm_outputs
cat llm_outputs/public.users.json
```
