use std::sync::Arc;

use tokio::net::TcpListener;

mod db;
mod protocol;
mod server;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

use db::Db;
use server::process;
use dashmap::DashMap;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    println!("Server listening on {}", addr);

    // Single lock-free DashMap - no sharding needed!
    let db: Db = Arc::new(DashMap::new());

    loop {
        let (socket, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                eprintln!("accept error: {err}");
                continue;
            }
        };
        let db_clone = db.clone();
        tokio::spawn(async move {
            if let Err(err) = process(socket, db_clone).await {
                eprintln!("connection error: {err}");
            }
        });
    }
}