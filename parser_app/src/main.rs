use std::env;
use std::fs;
use std::io::{self, Read};

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let sql = if args.len() > 1 {
        fs::read_to_string(&args[1])?
    } else {
        let mut s = String::new();
        io::stdin().read_to_string(&mut s)?;
        s
    };

    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, &sql)?;

    for stmt in statements {
        match &stmt {
            sqlparser::ast::Statement::CreateTable { .. } => {
                println!("CreateTable AST:\n{:#?}", stmt);
            }
            _ => {
                println!("Statement AST:\n{:#?}", stmt);
            }
        }
    }

    Ok(())
}
