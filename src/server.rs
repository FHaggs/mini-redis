use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::Result;
use crate::db::DbCommand;
use crate::protocol::{encode_response, Command, Frame, MAX_FRAME};

pub async fn process(
    mut socket: TcpStream,
    db_channel: mpsc::UnboundedSender<DbCommand>,
) -> Result<()> {
    let mut buf = BytesMut::with_capacity(MAX_FRAME);
    println!("New connection established");

    loop {
        match socket.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(err) => {
                eprintln!("read error: {err}");
                break;
            }
        }
        let mut frame = Frame::new();

        while let Some(command) = frame.try_parse(&mut buf) {
            let (tx, rx) = oneshot::channel();
            match command {
                Command::Get { req_id, key } => {
                    let cmd = DbCommand::Get { key, tx, req_id };
                    if db_channel.send(cmd).is_err() {
                        return Err("db manager dropped".into());
                    }
                }
                Command::Set { req_id, key, value } => {
                    let cmd = DbCommand::Set {
                        key,
                        value,
                        req_id,
                        tx,
                    };
                    if db_channel.send(cmd).is_err() {
                        return Err("db manager dropped".into());
                    }
                }
            }
            match rx.await {
                Ok(res) => {
                    let bytes = encode_response(res);
                    if let Err(err) = socket.write_all(&bytes).await {
                        return Err(err.into());
                    }
                }
                Err(_) => return Err("db response dropped".into()),
            }
        }
    }
    println!("Connection closed");
    Ok(())
}
