use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::query::QueryRequest;
use dotenvy::dotenv;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: delete_table <table_name> [table_name ...]");
        eprintln!("  Example: delete_table db_graph_nodes db_graph_edges");
        std::process::exit(1);
    }
    let table_names: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();

    let (config, project_id) = ClientConfig::new_with_auth().await?;
    let project_id = project_id
        .or_else(|| env::var("GOOGLE_CLOUD_PROJECT").ok())
        .expect("GOOGLE_CLOUD_PROJECT must be set");
    let dataset = env::var("BQ_DATASET").unwrap_or_else(|_| "ddl_graph".to_string());

    let client = Client::new(config).await?;

    for table_name in &table_names {
        let sql = format!(
            "DROP TABLE IF EXISTS `{}.{}.{}`",
            project_id, dataset, table_name
        );
        let req = QueryRequest {
            query: sql,
            ..Default::default()
        };
        client.job().query(&project_id, &req).await?;
        println!("Dropped table `{}.{}.{}`.", project_id, dataset, table_name);
    }

    Ok(())
}
