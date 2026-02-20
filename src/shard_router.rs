use crate::db::DbCommand;
use tokio::sync::mpsc::Sender;
use std::hash::{Hash, Hasher};
use ahash::AHasher;
use bytes::Bytes;

pub async fn send_to_shard(command: DbCommand, db_tx_channels: &[Sender<DbCommand>]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let num_shards = db_tx_channels.len();

    let key = match command {
        DbCommand::Set { ref key, value: _, req_id: _, resp_tx: _ } => key,
        DbCommand::Get { ref key, req_id: _, resp_tx: _ } => key,
        DbCommand::Delete { ref key, req_id: _, resp_tx: _ } => key,
    };

    let shard = shard_for_key(key, num_shards);
    // println!("Using shard {shard}");
    db_tx_channels[shard].send(command).await?;
    Ok(())


}

fn shard_for_key(key: &Bytes, num_shards: usize) -> usize {
    let mut hasher = AHasher::default();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}