use crate::Result;
use crate::db::DbCommand;
use crate::protocol::Response;
use crate::protocol::{Command, Frame, MAX_FRAME, encode_response};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{mpsc, oneshot};

pub async fn process(
    socket: TcpStream,
    db_channel: mpsc::UnboundedSender<DbCommand>,
) -> Result<()> {
    let (read_half, write_half) = socket.into_split();

    // Channel to send response receivers from reader to writer
    let (resp_tx, resp_rx) = mpsc::unbounded_channel::<oneshot::Receiver<Response>>();

    // Spawn writer task
    let writer_handle = tokio::spawn(writer_task(write_half, resp_rx));

    // Run reader in current task
    let reader_result = reader_task(read_half, db_channel, resp_tx).await;

    // Wait for writer to finish
    let writer_result = writer_handle.await;

    reader_result?;
    writer_result??;

    Ok(())
}

async fn reader_task(
    mut socket: OwnedReadHalf,
    db_channel: mpsc::UnboundedSender<DbCommand>,
    sender_to_writer: mpsc::UnboundedSender<oneshot::Receiver<Response>>,
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
            let (tx, rx) = oneshot::channel();
            let cmd = match command {
                Command::Get { req_id, key } => DbCommand::Get { key, tx, req_id },
                Command::Set { req_id, key, value } => DbCommand::Set {
                    key,
                    value,
                    req_id,
                    tx,
                },
            };
            if db_channel.send(cmd).is_err() {
                return Err("db manager dropped".into());
            }
            if sender_to_writer.send(rx).is_err() {
                return Err("writer dropped".into());
            }
        }
    }
    Ok(())
}

async fn writer_task(
    mut write_half: tokio::net::tcp::OwnedWriteHalf,
    mut resp_rx: mpsc::UnboundedReceiver<oneshot::Receiver<Response>>,
) -> Result<()> {
    let mut response_buf = BytesMut::with_capacity(4096);

    while let Some(rx) = resp_rx.recv().await {
        match rx.await {
            Ok(res) => {
                response_buf.extend_from_slice(&encode_response(res));

                // Drain any other ready responses (batching)
                while let Ok(rx) = resp_rx.try_recv() {
                    match rx.await {
                        Ok(res) => response_buf.extend_from_slice(&encode_response(res)),
                        Err(_) => return Err("db response dropped".into()),
                    }
                }

                // Write batch
                if let Err(err) = write_half.write_all(&response_buf).await {
                    return Err(err.into());
                }
                response_buf.clear();
            }
            Err(_) => return Err("db response dropped".into()),
        }
    }

    Ok(())
}
