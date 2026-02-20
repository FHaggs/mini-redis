use dashmap::DashMap;
use bytes::Bytes;
use tokio::sync::mpsc;
use std::sync::Arc;

pub type Db = Arc<DashMap<Bytes, Bytes>>;
use crate::protocol::Response;

#[derive(Debug)]
pub enum DbCommand {
    Set { key: Bytes, value: Bytes, req_id: u32, resp_tx: mpsc::Sender<Response> },
    Get { key: Bytes, req_id: u32, resp_tx: mpsc::Sender<Response> },
    Delete { key: Bytes, req_id: u32, resp_tx: mpsc::Sender<Response> },
}

// No more single-threaded async db_manager!
// Operations now happen directly in the connection handler
pub fn handle_command(db: &DashMap<Bytes, Bytes>, command: DbCommand) {
    match command {
        DbCommand::Get { key, req_id, resp_tx } => {
            let res = db.get(&key).map(|v| v.clone());
            let _ = match res {
                Some(value) => resp_tx.try_send(Response::Ok { req_id, value: Some(value) }),
                None => resp_tx.try_send(Response::NotFound { req_id }),
            };
        }
        DbCommand::Set { key, value, req_id, resp_tx } => {
            db.insert(key, value);
            let _ = resp_tx.try_send(Response::Ok { req_id, value: None });
        }
        DbCommand::Delete { key, req_id, resp_tx } => {
            let value = db.remove(&key).map(|(_, v)| v);
            let _ = match value {
                Some(value) => resp_tx.try_send(Response::Ok { req_id, value: Some(value) }),
                None => resp_tx.try_send(Response::NotFound { req_id }),
            };
        }
    }
}

