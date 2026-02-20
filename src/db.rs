use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc;

pub type Db = HashMap<Bytes, Bytes>;
use crate::protocol::Response;

#[derive(Debug)]
pub enum DbCommand {
    Set { key: Bytes, value: Bytes, req_id: u32, resp_tx: mpsc::Sender<Response> },
    Get { key: Bytes, req_id: u32, resp_tx: mpsc::Sender<Response> },
    Delete { key: Bytes, req_id: u32, resp_tx: mpsc::Sender<Response> },

}

pub async fn db_manager(mut db_rx: mpsc::Receiver<DbCommand>, mut db: Db) {
    println!("DB manager started");
    while let Some(command) = db_rx.recv().await {
        match command {
            DbCommand::Get { key, req_id, resp_tx } => {
                let res = db.get(&key).map(|v| v.clone());
                let _ = match res {
                    Some(value) => resp_tx.send(Response::Ok { req_id, value: Some(value) }).await,
                    None => resp_tx.send(Response::NotFound { req_id }).await,
                };
            }
            DbCommand::Set { key, value, req_id, resp_tx } => {
                db.insert(key, value);
                let _ = resp_tx.send(Response::Ok { req_id, value: None }).await;
            }
            DbCommand::Delete { key, req_id, resp_tx } => {
                let value = db.remove(&key);
                let _ = match value {
                    Some(value) => resp_tx.send(Response::Ok { req_id, value: Some(value) }).await,
                    None => resp_tx.send(Response::NotFound { req_id }).await,
                };
            }
        }
    }
}
