use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc;

pub type Db = HashMap<Bytes, Bytes>;
use crate::protocol::{create_response, Status};

use tokio::sync::oneshot::Sender;

#[derive(Debug)]
pub enum DbCommand {
    Set { key: Bytes, value: Bytes, req_id:  u32, tx: Sender<Bytes> },
    Get { key: Bytes, tx: Sender<Bytes>, req_id:  u32 },
}

pub async fn db_manager(mut db_rx: mpsc::UnboundedReceiver<DbCommand>, mut db: Db) {
    println!("DB manager started");
    while let Some(command) = db_rx.recv().await {
        match command {
            DbCommand::Get { key, tx, req_id } => {
                print!("DB GET: {:?} => ", key);
                let res = db.get(&key).map(|v| v.clone());
                let _ = match res {
                    Some(value) => tx.send(create_response(Status::Ok, req_id, Some(value))),
                    None => tx.send(create_response(Status::NotFound, req_id, None)),
                };
            }
            DbCommand::Set { key, value, req_id, tx } => {
                print!("DB SET: {:?} => {:?}\n", key, value);
                db.insert(key, value);
                let _ = tx.send(create_response(Status::Ok, req_id, None));
            }
        }
    }
}
