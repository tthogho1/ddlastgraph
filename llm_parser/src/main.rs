use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;
use std::error::Error;

use reqwest::header::AUTHORIZATION;
use serde_json::Value;
use dotenvy::dotenv;

/// Represents compact info extracted from AST dump blocks
#[derive(Default, Debug)]
struct TableInfo {
    columns: Vec<String>,       // e.g. "id integer NOT NULL"
    primary_key: Vec<String>,   // e.g. ["id"]
    foreign_keys: Vec<String>,  // e.g. ["dept_id -> public.departments(id)"]
    indexes: Vec<String>,       // e.g. ["idx_users_email USING btree (email)"]
    comments: Vec<String>,      // e.g. ["TABLE: Users table"]
    owner: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let args: Vec<String> = env::args().collect();
    let path = if args.len() > 1 { &args[1] } else { "user.txt" };
    let content = fs::read_to_string(path)?;

    // ---- Step 1: split into statement blocks ----
    let blocks = split_into_blocks(&content);

    // ---- Step 2: group by table, extract compact info ----
    let mut tables: HashMap<String, TableInfo> = HashMap::new();
    for block in &blocks {
        if let Some((mut table_name, kind)) = identify_block(block) {
            // Column-level comments have keys like public.users.email; merge into public.users
            if kind == "Comment" && block.contains("object_type: Column") {
                let parts: Vec<&str> = table_name.splitn(3, '.').collect();
                if parts.len() == 3 {
                    table_name = format!("{}.{}", parts[0], parts[1]);
                }
            }
            let info = tables.entry(table_name.clone()).or_default();
            match kind.as_str() {
                "CreateTable" => extract_columns(block, info),
                "PrimaryKey"  => extract_primary_key(block, info),
                "ForeignKey"  => extract_foreign_key(block, info),
                "CreateIndex" => extract_index(block, info),
                "Comment"     => extract_comment(block, info),
                "OwnerTo"     => extract_owner(block, info),
                _ => {}
            }
        }
    }

    let dry_run = args.iter().any(|a| a == "--dry-run");

    if tables.is_empty() {
        eprintln!("No table-related statements found in input.");
        return Ok(());
    }

    // ---- Step 3: per-table LLM call with compact prompt ----
    let api_key = env::var("OPENAI_API_KEY").map_err(|_| "Set OPENAI_API_KEY env var")?;
    let model = env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());
    let system = "You are an assistant that explains SQL database tables. \
                  Output JSON only, no extra commentary.";

    fs::create_dir_all("llm_outputs")?;

    for (table, info) in &tables {
        let summary = build_compact_summary(table, info);
        println!("=== Sending to LLM for table: {} ({} chars) ===", table, summary.len());
        println!("{}", summary);

        if dry_run {
            println!("--- TABLE: {} [DRY RUN — skipped LLM call] ---\n", table);
            continue;
        }

        let prompt = format!(
            "Below is a compact summary of a PostgreSQL table extracted from a schema dump.\n\
             Generate an English explanation of this table, including its purpose, each column, \
             constraints, keys, indexes and any comments. Return JSON only.\n\n\
             {}\n\n\
             Desired output format:\n\
             {{\"table\":\"...\", \"description\":\"...\", \
             \"columns\":[{{\"name\":\"...\",\"type\":\"...\",\"nullable\":true/false,\"comment\":null}}], \
             \"primary_key\":[...], \"foreign_keys\":[...], \"indexes\":[...], \"owner\":\"...\"}}",
            summary
        );

        let resp = send_openai(&api_key, &model, system, &prompt).await?;
        println!("--- TABLE: {} ---\n{}\n", table, resp);

        let safe_name = table.replace('/', "_").replace('"', "");
        let out_path = Path::new("llm_outputs").join(format!("{}.json", safe_name));
        fs::write(out_path, &resp)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Split the AST dump file into individual statement blocks
fn split_into_blocks(content: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current = String::new();
    for line in content.lines() {
        if (line.starts_with("CreateTable AST:") || line.starts_with("Statement AST:")) && !current.is_empty() {
            blocks.push(current.clone());
            current.clear();
        }
        current.push_str(line);
        current.push('\n');
    }
    if !current.is_empty() {
        blocks.push(current);
    }
    blocks
}

/// Identify block type and table name.  Returns (table_name, kind).
fn identify_block(block: &str) -> Option<(String, String)> {
    let table_name = extract_object_name(block)?;

    let kind = if block.contains("CreateTable(") {
        "CreateTable"
    } else if block.contains("CreateIndex(") {
        "CreateIndex"
    } else if block.contains("Comment {") {
        "Comment"
    } else if block.contains("ForeignKey(") || block.contains("ForeignKeyConstraint") {
        "ForeignKey"
    } else if block.contains("PrimaryKey(") || block.contains("PrimaryKeyConstraint") {
        "PrimaryKey"
    } else if block.contains("OwnerTo") {
        "OwnerTo"
    } else {
        return None;
    };
    Some((table_name, kind.to_string()))
}

/// Extract the first ObjectName (schema.table) from a block
fn extract_object_name(block: &str) -> Option<String> {
    // Look for `name: ObjectName(` pattern, then collect `value: "..."` entries
    let name_start = block.find("name: ObjectName(")?;
    let sub = &block[name_start..];
    // Find matching closing `)`
    let mut depth = 0i32;
    let mut end = 0;
    let paren_start = sub.find('(')?;
    for (i, ch) in sub[paren_start..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => { depth -= 1; if depth == 0 { end = paren_start + i; break; } }
            _ => {}
        }
    }
    let name_section = &sub[..end + 1];
    let parts = extract_values(name_section);
    if parts.is_empty() { return None; }
    Some(parts.join("."))
}

/// Extract all `value: "..."` strings from a text section
fn extract_values(text: &str) -> Vec<String> {
    let mut vals = Vec::new();
    let mut start = 0;
    while let Some(pos) = text[start..].find("value: \"") {
        let s = start + pos + 8; // len of `value: "`
        if let Some(end) = text[s..].find('"') {
            vals.push(text[s..s + end].to_string());
            start = s + end + 1;
        } else {
            break;
        }
    }
    vals
}

fn extract_columns(block: &str, info: &mut TableInfo) {
    // Find the `columns: [` section and parse each ColumnDef
    if let Some(cols_start) = block.find("columns: [") {
        let sub = &block[cols_start..];
        // Split by "ColumnDef {" to get individual columns
        for chunk in sub.split("ColumnDef {").skip(1) {
            let vals = extract_values(chunk);
            let col_name = vals.first().cloned().unwrap_or_default();

            // data_type — clean up e.g. "Integer(" -> "integer", "CharacterVarying(" -> "character varying(100)"
            let dtype_raw = extract_after(chunk, "data_type: ")
                .unwrap_or_else(|| "unknown".to_string());
            let dtype = clean_data_type(&dtype_raw, chunk);

            // nullable
            let not_null = chunk.contains("option: NotNull");
            let nullable_str = if not_null { "NOT NULL" } else { "NULL" };

            info.columns.push(format!("{} {} {}", col_name, dtype, nullable_str));
        }
    }
}

fn extract_primary_key(block: &str, info: &mut TableInfo) {
    // Find PrimaryKeyConstraint columns
    if let Some(pk_start) = block.find("PrimaryKeyConstraint") {
        let sub = &block[pk_start..];
        if let Some(cols_start) = sub.find("columns: [") {
            let cols_sub = &sub[cols_start..];
            // find closing ]
            if let Some(end) = cols_sub.find(']') {
                let section = &cols_sub[..end];
                for v in extract_values(section) {
                    if !info.primary_key.contains(&v) {
                        info.primary_key.push(v);
                    }
                }
            }
        }
    }
}

fn extract_foreign_key(block: &str, info: &mut TableInfo) {
    if let Some(fk_start) = block.find("ForeignKeyConstraint") {
        let sub = &block[fk_start..];
        // local columns
        let local_cols = if let Some(c) = sub.find("columns: [") {
            let s = &sub[c..];
            if let Some(e) = s.find(']') { extract_values(&s[..e]) } else { vec![] }
        } else { vec![] };

        // foreign_table — only take values inside the ObjectName(...) block
        let foreign_table = if let Some(ft) = sub.find("foreign_table: ObjectName(") {
            let s = &sub[ft..];
            // find closing ) for ObjectName
            let mut depth = 0i32;
            let mut end_pos = 0;
            if let Some(paren) = s.find('(') {
                for (i, ch) in s[paren..].char_indices() {
                    match ch {
                        '(' => depth += 1,
                        ')' => { depth -= 1; if depth == 0 { end_pos = paren + i; break; } }
                        _ => {}
                    }
                }
            }
            let ft_section = &s[..end_pos + 1];
            extract_values(ft_section).join(".")
        } else { String::new() };

        // referred_columns
        let ref_cols = if let Some(rc) = sub.find("referred_columns: [") {
            let s = &sub[rc..];
            if let Some(e) = s.find(']') { extract_values(&s[..e]) } else { vec![] }
        } else { vec![] };

        info.foreign_keys.push(format!(
            "({}) -> {}({})",
            local_cols.join(", "),
            foreign_table,
            ref_cols.join(", ")
        ));
    }
}

fn extract_index(block: &str, info: &mut TableInfo) {
    let idx_name = if let Some(n) = block.find("name: Some(") {
        let sub = &block[n..];
        extract_values(sub).first().cloned().unwrap_or_default()
    } else { String::new() };

    let using = extract_after(block, "using: Some(")
        .map(|s| s.trim_end_matches(|c: char| c == ',' || c == ')' || c.is_whitespace()).to_string())
        .unwrap_or_default();

    // index columns
    let cols: Vec<String> = if let Some(c) = block.find("columns: [") {
        let sub = &block[c..];
        extract_values(sub)
    } else { vec![] };

    info.indexes.push(format!("{} USING {} ({})", idx_name, using, cols.join(", ")));
}

fn extract_comment(block: &str, info: &mut TableInfo) {
    let obj_type = if block.contains("object_type: Table") { "TABLE" }
                   else if block.contains("object_type: Column") { "COLUMN" }
                   else { "UNKNOWN" };

    let comment_text = extract_quoted_after(block, "comment: Some(")
        .unwrap_or_default();

    let target = if obj_type == "COLUMN" {
        // last value is the column name
        let vals = extract_values(block);
        vals.last().cloned().unwrap_or_default()
    } else {
        "TABLE".to_string()
    };

    info.comments.push(format!("{} {}: {}", obj_type, target, comment_text));
}

fn extract_owner(block: &str, info: &mut TableInfo) {
    if let Some(pos) = block.find("new_owner:") {
        let sub = &block[pos..];
        let vals = extract_values(sub);
        if let Some(owner) = vals.first() {
            info.owner = Some(owner.clone());
        }
    }
}

/// Extract text after a marker up to end of line
fn extract_after(text: &str, marker: &str) -> Option<String> {
    let pos = text.find(marker)?;
    let start = pos + marker.len();
    let rest = &text[start..];
    let end = rest.find('\n').unwrap_or(rest.len());
    let val = rest[..end].trim().to_string();
    Some(val)
}

/// Extract a quoted string after a marker like `comment: Some(`
fn extract_quoted_after(text: &str, marker: &str) -> Option<String> {
    let pos = text.find(marker)?;
    let rest = &text[pos + marker.len()..];
    if let Some(q1) = rest.find('"') {
        let after = &rest[q1 + 1..];
        if let Some(q2) = after.find('"') {
            return Some(after[..q2].to_string());
        }
    }
    None
}

/// Build a compact human-readable summary for one table
fn build_compact_summary(table: &str, info: &TableInfo) -> String {
    let mut s = format!("Table: {}\n", table);
    if !info.columns.is_empty() {
        s.push_str("Columns:\n");
        for c in &info.columns {
            s.push_str(&format!("  - {}\n", c));
        }
    }
    if !info.primary_key.is_empty() {
        s.push_str(&format!("Primary Key: ({})\n", info.primary_key.join(", ")));
    }
    for fk in &info.foreign_keys {
        s.push_str(&format!("Foreign Key: {}\n", fk));
    }
    for idx in &info.indexes {
        s.push_str(&format!("Index: {}\n", idx));
    }
    for cmt in &info.comments {
        s.push_str(&format!("Comment: {}\n", cmt));
    }
    if let Some(ref owner) = info.owner {
        s.push_str(&format!("Owner: {}\n", owner));
    }
    s
}

/// Clean raw data_type string from AST dump into readable SQL type
fn clean_data_type(raw: &str, chunk: &str) -> String {
    // raw looks like "Integer(" or "CharacterVarying("
    let base = raw.trim_end_matches(|c: char| c == '(' || c == ')' || c == ',');
    let name = match base {
        "Integer" => "integer",
        "Int" => "int",
        "BigInt" => "bigint",
        "SmallInt" => "smallint",
        "CharacterVarying" => "character varying",
        "Varchar" => "varchar",
        "Char" | "Character" => "char",
        "Text" => "text",
        "Boolean" | "Bool" => "boolean",
        "Date" => "date",
        "Timestamp" => "timestamp",
        "TimestampTz" => "timestamptz",
        "Numeric" | "Decimal" => "numeric",
        "Real" | "Float4" => "real",
        "DoublePrecision" | "Float8" => "double precision",
        "Serial" => "serial",
        "BigSerial" => "bigserial",
        "Uuid" => "uuid",
        "Json" => "json",
        "Jsonb" => "jsonb",
        "Bytea" => "bytea",
        other => other,
    };
    // Try to find length like `length: 100`
    if let Some(pos) = chunk.find("length: ") {
        let rest = &chunk[pos + 8..];
        if let Some(end) = rest.find(|c: char| !c.is_ascii_digit()) {
            let len = &rest[..end];
            if !len.is_empty() {
                return format!("{}({})", name, len);
            }
        }
    }
    name.to_string()
}

async fn send_openai(api_key: &str, model: &str, system: &str, user_prompt: &str) -> Result<String, Box<dyn Error>> {
    let client = reqwest::Client::new();
    let req_body = serde_json::json!({
        "model": model,
        "messages": [
            {"role":"system","content": system},
            {"role":"user","content": user_prompt}
        ],
        "max_completion_tokens": 1200
    });

    let resp_text = client
        .post("https://api.openai.com/v1/chat/completions")
        .header(AUTHORIZATION, format!("Bearer {}", api_key))
        .json(&req_body)
        .send()
        .await?
        .text()
        .await?;

    // Try to extract the assistant message; if parsing fails, return raw text
    if let Ok(v) = serde_json::from_str::<Value>(&resp_text) {
        if let Some(s) = v["choices"][0]["message"]["content"].as_str() {
            return Ok(s.to_string());
        }
    }
    Ok(resp_text)
}
