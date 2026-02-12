use std::{collections::HashMap, sync::Arc};

use tokio::{net::TcpListener, sync::mpsc};

mod db;
mod protocol;
mod server;
mod shard_router;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

use db::{db_manager, Db, DbCommand};
use server::process;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    println!("Server listening on {}", addr);

    let num_shards = 6;
    let mut db_tx_channels = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        let (db_tx, db_rx) = mpsc::unbounded_channel::<DbCommand>();
        let db: Db = HashMap::new();
        tokio::spawn(db_manager(db_rx, db));
        db_tx_channels.push(db_tx);
    }
    let db_tx_channels = Arc::new(db_tx_channels);

    loop {
        let (socket, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                eprintln!("accept error: {err}");
                continue;
            }
        };
        let db_tx_channels_clone = db_tx_channels.clone();
        tokio::spawn(async move {
            if let Err(err) = process(socket, db_tx_channels_clone).await {
                eprintln!("connection error: {err}");
            }
        });
    }
}