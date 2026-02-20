use std::sync::Arc;

use crate::Result;
use crate::db::DbCommand;
use crate::protocol::Response;
use crate::protocol::{Command, Frame, MAX_FRAME, encode_response};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::mpsc;

use crate::shard_router::send_to_shard;

pub async fn process(
    socket: TcpStream,
    shards: Arc<Vec<mpsc::Sender<DbCommand>>>,
) -> Result<()> {
    let (read_half, write_half) = socket.into_split();

    // Single response channel for this connection - no per-request allocations
    let (resp_tx, resp_rx) = mpsc::channel::<Response>(512);

    // Spawn writer task
    let writer_handle = tokio::spawn(writer_task(write_half, resp_rx));

    // Run reader in current task
    let reader_result = reader_task(read_half, &shards, resp_tx).await;

    // Wait for writer to finish
    let writer_result = writer_handle.await;

    reader_result?;
    writer_result??;

    Ok(())
}

async fn reader_task(
    mut socket: OwnedReadHalf,
    shards: &[mpsc::Sender<DbCommand>],
    resp_tx: mpsc::Sender<Response>,
) -> Result<()> {
    let mut frame = Frame::new();
    let mut buf = BytesMut::with_capacity(MAX_FRAME);
    loop {
        match socket.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(err) => {
                eprintln!("read error: {err}");
                return Err(err.into());
            }
        }
        while let Some(command) = frame.try_parse(&mut buf) {
            // Clone is cheap - just Arc increment
            let cmd = match command {
                Command::Get { req_id, key } => DbCommand::Get {
                    key,
                    req_id,
                    resp_tx: resp_tx.clone(),
                },
                Command::Set { req_id, key, value } => DbCommand::Set {
                    key,
                    value,
                    req_id,
                    resp_tx: resp_tx.clone(),
                },
                Command::Delete { req_id, key } => DbCommand::Delete {
                    key,
                    req_id,
                    resp_tx: resp_tx.clone(),
                }
                
            };
            if send_to_shard(cmd, shards).await.is_err() {
                return Err("db manager dropped".into());
            }
        }
    }
    Ok(())
}

async fn writer_task(
    mut write_half: tokio::net::tcp::OwnedWriteHalf,
    mut resp_rx: mpsc::Receiver<Response>,
) -> Result<()> {
    let mut response_buf = BytesMut::with_capacity(4096);

    while let Some(res) = resp_rx.recv().await {
        response_buf.extend_from_slice(&encode_response(res));

        // Drain any other ready responses (batching)
        while let Ok(res) = resp_rx.try_recv() {
            response_buf.extend_from_slice(&encode_response(res));
        }

        // Write batch
        if let Err(err) = write_half.write_all(&response_buf).await {
            return Err(err.into());
        }
        response_buf.clear();
    }

    Ok(())
}
