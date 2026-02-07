use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc;

pub type Db = HashMap<Bytes, Bytes>;

#[derive(Debug)]
pub enum DbCommand {
    Set { key: Bytes, value: Bytes },
    Get { key: Bytes, tx: tokio::sync::oneshot::Sender<Option<Bytes>> },
}

pub async fn db_manager(mut db_rx: mpsc::UnboundedReceiver<DbCommand>, mut db: Db) {
    println!("DB manager started");
    while let Some(command) = db_rx.recv().await {
        match command {
            DbCommand::Get { key, tx } => {
                print!("DB GET: {:?} => ", key);
                let res = db.get(&key).map(|v| v.clone());
                let _ = tx.send(res);
            }
            DbCommand::Set { key, value } => {
                print!("DB SET: {:?} => {:?}\n", key, value);
                db.insert(key, value);
            }
        }
    }
}
