use anyhow::Result;
use dotenvy::dotenv;
use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::query::QueryRequest;
use regex::Regex;
use std::collections::HashSet;
use std::env;
use std::fs;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

struct NodeRow {
    node_id: String,
    node_type: String,
    schema_name: Option<String>,
    table_name: Option<String>,
    column_name: Option<String>,
    description: Option<String>,
    properties: String, // JSON string
}

struct EdgeRow {
    src_node_id: String,
    dst_node_id: String,
    edge_type: String,
    confidence: String,
    properties: String, // JSON string
}

// ---------------------------------------------------------------------------
// Block splitting  (same strategy as llm_parser)
// ---------------------------------------------------------------------------

fn split_into_blocks(content: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current = String::new();
    for line in content.lines() {
        if (line.starts_with("CreateTable AST:")
            || line.starts_with("Statement AST:"))
            && !current.is_empty()
        {
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

// ---------------------------------------------------------------------------
// Regex helpers – extract `value: "..."` entries from a section
// ---------------------------------------------------------------------------

fn extract_values(text: &str) -> Vec<String> {
    let re = Regex::new(r#"value:\s*"([^"]+)""#).unwrap();
    re.captures_iter(text)
        .map(|c| c[1].to_string())
        .collect()
}

/// Extract the first ObjectName (schema.table or schema.table.column) from a
/// block by finding `name: ObjectName(` and collecting `value:` entries inside.
fn extract_object_name(block: &str) -> Option<Vec<String>> {
    let start = block.find("name: ObjectName(")?;
    let sub = &block[start..];
    let paren_start = sub.find('(')?;
    let mut depth = 0i32;
    let mut end = 0;
    for (i, ch) in sub[paren_start..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = paren_start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    let section = &sub[..end + 1];
    let vals = extract_values(section);
    if vals.is_empty() {
        None
    } else {
        Some(vals)
    }
}

/// Extract data_type text from a ColumnDef block (rough text between `data_type:` and `options:`)
fn extract_data_type(col_block: &str) -> String {
    if let Some(start) = col_block.find("data_type:") {
        let rest = &col_block[start + 10..];
        // find the next `options:` marker
        let end = rest.find("options:").unwrap_or(rest.len());
        let raw = rest[..end].trim();
        // simplify: CharacterVarying(Some(IntegerLength { length: 100 ... })) -> varchar(100)
        // IMPORTANT: check CharacterVarying BEFORE Integer (IntegerLength contains "Integer")
        if raw.contains("CharacterVarying") {
            let re = Regex::new(r"length:\s*(\d+)").unwrap();
            if let Some(cap) = re.captures(raw) {
                return format!("varchar({})", &cap[1]);
            }
            return "varchar".to_string();
        }
        if raw.contains("Integer") {
            return "integer".to_string();
        }
        if raw.contains("Text") {
            return "text".to_string();
        }
        if raw.contains("Boolean") {
            return "boolean".to_string();
        }
        // fallback: return trimmed raw
        raw.lines().next().unwrap_or("unknown").trim_end_matches(',').to_string()
    } else {
        "unknown".to_string()
    }
}

/// Check if a ColumnDef block contains `option: NotNull`
fn is_not_null(col_block: &str) -> bool {
    col_block.contains("option: NotNull")
}

// ---------------------------------------------------------------------------
// Block parsers
// ---------------------------------------------------------------------------

fn parse_create_table(
    block: &str,
    nodes: &mut Vec<NodeRow>,
    edges: &mut Vec<EdgeRow>,
    seen_schemas: &mut HashSet<String>,
) {
    let parts = match extract_object_name(block) {
        Some(p) if p.len() >= 2 => p,
        _ => return,
    };
    let schema = &parts[0];
    let table = &parts[1];
    let schema_node_id = format!("schema:{}", schema);
    let table_node_id = format!("table:{}.{}", schema, table);

    // Schema node (deduplicate)
    if seen_schemas.insert(schema.clone()) {
        nodes.push(NodeRow {
            node_id: schema_node_id.clone(),
            node_type: "Schema".into(),
            schema_name: Some(schema.clone()),
            table_name: None,
            column_name: None,
            description: None,
            properties: "{}".into(),
        });
    }

    // Table node
    nodes.push(NodeRow {
        node_id: table_node_id.clone(),
        node_type: "Table".into(),
        schema_name: Some(schema.clone()),
        table_name: Some(table.clone()),
        column_name: None,
        description: None,
        properties: "{}".into(),
    });

    // HAS_TABLE edge
    edges.push(EdgeRow {
        src_node_id: schema_node_id.clone(),
        dst_node_id: table_node_id.clone(),
        edge_type: "HAS_TABLE".into(),
        confidence: "explicit".into(),
        properties: "{}".into(),
    });

    // Parse columns – split on `ColumnDef {`
    let col_blocks: Vec<&str> = block.split("ColumnDef {").collect();
    for cb in col_blocks.iter().skip(1) {
        // column name
        let col_vals = extract_values(cb);
        if col_vals.is_empty() {
            continue;
        }
        let col_name = &col_vals[0];
        let data_type = extract_data_type(cb);
        let nullable = !is_not_null(cb);

        let col_node_id = format!("column:{}.{}.{}", schema, table, col_name);

        let props = serde_json::json!({
            "data_type": data_type,
            "nullable": nullable,
        })
        .to_string();

        nodes.push(NodeRow {
            node_id: col_node_id.clone(),
            node_type: "Column".into(),
            schema_name: Some(schema.clone()),
            table_name: Some(table.clone()),
            column_name: Some(col_name.clone()),
            description: None,
            properties: props,
        });

        edges.push(EdgeRow {
            src_node_id: table_node_id.clone(),
            dst_node_id: col_node_id,
            edge_type: "HAS_COLUMN".into(),
            confidence: "explicit".into(),
            properties: "{}".into(),
        });
    }
}

fn parse_primary_key(block: &str, edges: &mut Vec<EdgeRow>) {
    let parts = match extract_object_name(block) {
        Some(p) if p.len() >= 2 => p,
        _ => return,
    };
    let schema = &parts[0];
    let table = &parts[1];
    let table_node_id = format!("table:{}.{}", schema, table);

    // Extract PK constraint name
    let pk_name_re = Regex::new(r#"PrimaryKeyConstraint\s*\{[^}]*?name:\s*Some\(\s*Ident\s*\{\s*value:\s*"([^"]+)""#).unwrap();
    let pk_name = pk_name_re
        .captures(block)
        .map(|c| c[1].to_string())
        .unwrap_or_default();

    // Extract PK column names (inside PrimaryKeyConstraint ... columns: [ ... ])
    let pk_col_re = Regex::new(
        r#"PrimaryKeyConstraint[\s\S]*?columns:\s*\[([\s\S]*?)\]\s*,"#,
    )
    .unwrap();
    if let Some(cap) = pk_col_re.captures(block) {
        let cols_section = &cap[1];
        let col_vals = extract_values(cols_section);
        for col in &col_vals {
            let col_node_id = format!("column:{}.{}.{}", schema, table, col);
            let props = serde_json::json!({
                "constraint_name": pk_name,
                "constraint_type": "PRIMARY KEY",
            })
            .to_string();

            edges.push(EdgeRow {
                src_node_id: table_node_id.clone(),
                dst_node_id: col_node_id,
                edge_type: "PRIMARY_KEY".into(),
                confidence: "explicit".into(),
                properties: props,
            });
        }
    }
}

fn parse_foreign_key(block: &str, edges: &mut Vec<EdgeRow>) {
    let parts = match extract_object_name(block) {
        Some(p) if p.len() >= 2 => p,
        _ => return,
    };
    let src_schema = &parts[0];
    let src_table = &parts[1];

    // FK constraint name
    let fk_name_re = Regex::new(r#"ForeignKeyConstraint\s*\{[^}]*?name:\s*Some\(\s*Ident\s*\{\s*value:\s*"([^"]+)""#).unwrap();
    let fk_name = fk_name_re
        .captures(block)
        .map(|c| c[1].to_string())
        .unwrap_or_default();

    // Source columns (inside ForeignKeyConstraint ... columns: [...])
    let src_cols_re = Regex::new(
        r#"ForeignKeyConstraint[\s\S]*?columns:\s*\[([\s\S]*?)\]\s*,\s*foreign_table"#,
    )
    .unwrap();
    let src_cols: Vec<String> = src_cols_re
        .captures(block)
        .map(|c| extract_values(&c[1]))
        .unwrap_or_default();

    // Foreign table
    let ft_re = Regex::new(
        r#"foreign_table:\s*ObjectName\(\s*\[([\s\S]*?)\]\s*,?\s*\)"#,
    )
    .unwrap();
    let foreign_parts: Vec<String> = ft_re
        .captures(block)
        .map(|c| extract_values(&c[1]))
        .unwrap_or_default();

    // Referred columns
    let ref_cols_re = Regex::new(
        r#"referred_columns:\s*\[([\s\S]*?)\]\s*,"#,
    )
    .unwrap();
    let ref_cols: Vec<String> = ref_cols_re
        .captures(block)
        .map(|c| extract_values(&c[1]))
        .unwrap_or_default();

    if foreign_parts.len() >= 2 {
        let dst_schema = &foreign_parts[0];
        let dst_table = &foreign_parts[1];

        for (i, src_col) in src_cols.iter().enumerate() {
            let dst_col = ref_cols.get(i).cloned().unwrap_or_else(|| "id".into());
            let src_node = format!("column:{}.{}.{}", src_schema, src_table, src_col);
            let dst_node = format!("column:{}.{}.{}", dst_schema, dst_table, dst_col);

            let props = serde_json::json!({
                "constraint_name": fk_name,
                "src_table": format!("{}.{}", src_schema, src_table),
                "dst_table": format!("{}.{}", dst_schema, dst_table),
            })
            .to_string();

            edges.push(EdgeRow {
                src_node_id: src_node,
                dst_node_id: dst_node,
                edge_type: "REFERENCES".into(),
                confidence: "explicit".into(),
                properties: props,
            });
        }
    }
}

fn parse_create_index(block: &str, edges: &mut Vec<EdgeRow>) {
    // Index name
    let idx_name_re = Regex::new(r#"CreateIndex\s*\{[\s\S]*?name:\s*Some\(\s*ObjectName\(\s*\[([\s\S]*?)\]\s*,?\s*\)"#).unwrap();
    let idx_name = idx_name_re
        .captures(block)
        .and_then(|c| extract_values(&c[1]).into_iter().next())
        .unwrap_or_default();

    // Table name (table_name: ObjectName(...))
    let tbl_re = Regex::new(r#"table_name:\s*ObjectName\(\s*\[([\s\S]*?)\]\s*,?\s*\)"#).unwrap();
    let tbl_parts: Vec<String> = tbl_re
        .captures(block)
        .map(|c| extract_values(&c[1]))
        .unwrap_or_default();

    if tbl_parts.len() < 2 {
        return;
    }
    let schema = &tbl_parts[0];
    let table = &tbl_parts[1];
    let table_node_id = format!("table:{}.{}", schema, table);

    // Using method
    let using_re = Regex::new(r#"using:\s*Some\(\s*(\w+)"#).unwrap();
    let using = using_re
        .captures(block)
        .map(|c| c[1].to_string())
        .unwrap_or_else(|| "btree".into());

    // Index columns
    let idx_cols_section_re =
        Regex::new(r#"CreateIndex\s*\{[\s\S]*?columns:\s*\[([\s\S]*?)\]\s*,"#).unwrap();
    let idx_cols: Vec<String> = idx_cols_section_re
        .captures(block)
        .map(|c| extract_values(&c[1]))
        .unwrap_or_default();

    for col in &idx_cols {
        let col_node_id = format!("column:{}.{}.{}", schema, table, col);
        let props = serde_json::json!({
            "index_name": idx_name,
            "using": using,
        })
        .to_string();

        edges.push(EdgeRow {
            src_node_id: table_node_id.clone(),
            dst_node_id: col_node_id,
            edge_type: "HAS_INDEX".into(),
            confidence: "explicit".into(),
            properties: props,
        });
    }
}

fn parse_comment(block: &str, nodes: &mut Vec<NodeRow>) {
    // Determine object_type: Table or Column
    let is_column = block.contains("object_type: Column");

    // Extract object_name parts
    let on_re = Regex::new(r#"object_name:\s*ObjectName\(\s*\[([\s\S]*?)\]\s*,?\s*\)"#).unwrap();
    let parts: Vec<String> = on_re
        .captures(block)
        .map(|c| extract_values(&c[1]))
        .unwrap_or_default();

    // Extract comment text
    let comment_re = Regex::new(r#"comment:\s*Some\(\s*"([^"]+)""#).unwrap();
    let comment = comment_re.captures(block).map(|c| c[1].to_string());

    if parts.len() >= 2 {
        let schema = &parts[0];
        let table = &parts[1];

        if is_column && parts.len() >= 3 {
            let col = &parts[2];
            let node_id = format!("column:{}.{}.{}", schema, table, col);
            // Update existing node description by matching node_id
            for n in nodes.iter_mut() {
                if n.node_id == node_id {
                    n.description = comment.clone();
                    return;
                }
            }
        } else {
            let node_id = format!("table:{}.{}", schema, table);
            for n in nodes.iter_mut() {
                if n.node_id == node_id {
                    n.description = comment.clone();
                    return;
                }
            }
        }
    }
}

fn parse_owner(block: &str, nodes: &mut Vec<NodeRow>) {
    let parts = match extract_object_name(block) {
        Some(p) if p.len() >= 2 => p,
        _ => return,
    };
    let schema = &parts[0];
    let table = &parts[1];
    let table_node_id = format!("table:{}.{}", schema, table);

    // Extract owner name
    let owner_re = Regex::new(r#"new_owner:\s*Ident\(\s*Ident\s*\{\s*value:\s*"([^"]+)""#).unwrap();
    let owner = owner_re
        .captures(block)
        .map(|c| c[1].to_string())
        .unwrap_or_default();

    // Merge owner into existing table node properties
    for n in nodes.iter_mut() {
        if n.node_id == table_node_id {
            let mut props: serde_json::Value =
                serde_json::from_str(&n.properties).unwrap_or(serde_json::json!({}));
            props["owner"] = serde_json::Value::String(owner.clone());
            n.properties = props.to_string();
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// SQL value escaping
// ---------------------------------------------------------------------------

fn sql_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

fn sql_str(s: &str) -> String {
    format!("'{}'", sql_escape(s))
}

fn sql_opt(s: &Option<String>) -> String {
    match s {
        Some(v) => sql_str(v),
        None => "NULL".into(),
    }
}

// ---------------------------------------------------------------------------
// BigQuery helpers
// ---------------------------------------------------------------------------

async fn run_query(client: &Client, project_id: &str, sql: &str) -> Result<()> {
    let req = QueryRequest {
        query: sql.to_string(),
        ..Default::default()
    };
    client.job().query(project_id, &req).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let (config, proj) = ClientConfig::new_with_auth().await?;
    let project_id = proj
        .or_else(|| env::var("GOOGLE_CLOUD_PROJECT").ok())
        .expect("GOOGLE_CLOUD_PROJECT must be set");
    let dataset = env::var("BQ_DATASET").unwrap_or_else(|_| "ddl_graph".to_string());
    let client = Client::new(config).await?;

    // Input file (default: ../user.txt relative to working directory)
    let path = env::args().nth(1).unwrap_or_else(|| "user.txt".to_string());
    let content = fs::read_to_string(&path)?;
    println!("Read {} bytes from {}", content.len(), path);

    // ---- Parse ----
    let blocks = split_into_blocks(&content);
    let mut nodes: Vec<NodeRow> = Vec::new();
    let mut edges: Vec<EdgeRow> = Vec::new();
    let mut seen_schemas: HashSet<String> = HashSet::new();

    for block in &blocks {
        if block.contains("CreateTable(") {
            parse_create_table(block, &mut nodes, &mut edges, &mut seen_schemas);
        } else if block.contains("PrimaryKey(") || block.contains("PrimaryKeyConstraint") {
            // Must check PK before FK since FK blocks don't contain PrimaryKey
            if !block.contains("ForeignKey(") {
                parse_primary_key(block, &mut edges);
            }
        }
        if block.contains("ForeignKey(") || block.contains("ForeignKeyConstraint") {
            parse_foreign_key(block, &mut edges);
        }
        if block.contains("CreateIndex(") {
            parse_create_index(block, &mut edges);
        }
        if block.contains("Comment {") {
            parse_comment(block, &mut nodes);
        }
        if block.contains("OwnerTo") {
            parse_owner(block, &mut nodes);
        }
    }

    println!("Parsed: {} nodes, {} edges", nodes.len(), edges.len());

    // ---- Dry-run mode ----
    let dry_run = env::args().any(|a| a == "--dry-run");

    // ---- Delete existing rows (idempotent re-import) ----
    if !dry_run {
        let del_edges = format!(
            "DELETE FROM `{}.{}.db_graph_edges` WHERE TRUE",
            project_id, dataset
        );
        let del_nodes = format!(
            "DELETE FROM `{}.{}.db_graph_nodes` WHERE TRUE",
            project_id, dataset
        );
        run_query(&client, &project_id, &del_edges).await?;
        run_query(&client, &project_id, &del_nodes).await?;
        println!("Cleared existing rows.");
    }

    // ---- Insert nodes ----
    if !nodes.is_empty() {
        let values: Vec<String> = nodes
            .iter()
            .map(|n| {
                format!(
                    "({},{},NULL,{},{},{},{},PARSE_JSON({}))",
                    sql_str(&n.node_id),
                    sql_str(&n.node_type),
                    sql_opt(&n.schema_name),
                    sql_opt(&n.table_name),
                    sql_opt(&n.column_name),
                    sql_opt(&n.description),
                    sql_str(&n.properties),
                )
            })
            .collect();

        let sql = format!(
            "INSERT INTO `{}.{}.db_graph_nodes` \
             (node_id, node_type, db_name, schema_name, table_name, column_name, description, properties) \
             VALUES {}",
            project_id,
            dataset,
            values.join(",\n")
        );

        if dry_run {
            println!("[DRY RUN] INSERT nodes:\n{}", sql);
        } else {
            run_query(&client, &project_id, &sql).await?;
            println!("Inserted {} nodes.", nodes.len());
        }
    }

    // ---- Insert edges ----
    if !edges.is_empty() {
        let values: Vec<String> = edges
            .iter()
            .map(|e| {
                format!(
                    "({},{},{},{},PARSE_JSON({}))",
                    sql_str(&e.src_node_id),
                    sql_str(&e.dst_node_id),
                    sql_str(&e.edge_type),
                    sql_str(&e.confidence),
                    sql_str(&e.properties),
                )
            })
            .collect();

        let sql = format!(
            "INSERT INTO `{}.{}.db_graph_edges` \
             (src_node_id, dst_node_id, edge_type, confidence, properties) \
             VALUES {}",
            project_id,
            dataset,
            values.join(",\n")
        );

        if dry_run {
            println!("[DRY RUN] INSERT edges:\n{}", sql);
        } else {
            run_query(&client, &project_id, &sql).await?;
            println!("Inserted {} edges.", edges.len());
        }
    }

    println!("Done.");
    Ok(())
}
