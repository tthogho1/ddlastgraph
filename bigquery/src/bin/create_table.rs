use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::query::QueryRequest;
use dotenvy::dotenv;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // Create client config with auth from GOOGLE_APPLICATION_CREDENTIALS
    let (config, project_id) = ClientConfig::new_with_auth().await?;
    let project_id = project_id
        .or_else(|| env::var("GOOGLE_CLOUD_PROJECT").ok())
        .expect("GOOGLE_CLOUD_PROJECT must be set");

    let dataset = env::var("BQ_DATASET").unwrap_or_else(|_| "ddl_graph".to_string());

    let client = Client::new(config).await?;

    // Create dataset if not exists
    let create_dataset = format!(
        "CREATE SCHEMA IF NOT EXISTS `{project_id}.{dataset}`"
    );
    run_query(&client, &project_id, &create_dataset).await?;
    println!("Dataset `{}.{}` ready.", project_id, dataset);

    // Create db_graph_nodes table (with embedding column)
    let create_nodes = format!(r#"
        CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.db_graph_nodes` (
            node_id       STRING NOT NULL,
            node_type     STRING NOT NULL,
            db_name       STRING,
            schema_name   STRING,
            table_name    STRING,
            column_name   STRING,
            description   STRING,
            embedding     ARRAY<FLOAT64>,
            properties    JSON
        )
    "#);
    run_query(&client, &project_id, &create_nodes).await?;
    println!("Table `{}.{}.db_graph_nodes` created.", project_id, dataset);

    // Create db_graph_edges table
    let create_edges = format!(r#"
        CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.db_graph_edges` (
            src_node_id   STRING NOT NULL,
            dst_node_id   STRING NOT NULL,
            edge_type     STRING NOT NULL,
            confidence    STRING,
            properties    JSON
        )
    "#);
    run_query(&client, &project_id, &create_edges).await?;
    println!("Table `{}.{}.db_graph_edges` created.", project_id, dataset);

    Ok(())
}

async fn run_query(
    client: &Client,
    project_id: &str,
    sql: &str,
) -> anyhow::Result<()> {
    let req = QueryRequest {
        query: sql.to_string(),
        ..Default::default()
    };
    client.job().query(project_id, &req).await?;
    Ok(())
}
