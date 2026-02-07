use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::db::DbCommand;
use crate::protocol::{Frame, Command, MAX_FRAME};
use crate::Result;

pub async fn process(mut socket: TcpStream, db_channel: mpsc::UnboundedSender<DbCommand>) -> Result<()> {
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
            match command {
                Command::Get { req_id, key } => {
                    let (tx, rx) = oneshot::channel();
                    let cmd = DbCommand::Get { key, tx, req_id };
                    if db_channel.send(cmd).is_err() {
                        return Err("db manager dropped".into());
                    }
                    match rx.await {
                        Ok(res) => {
                            if let Err(err) = socket.write_all(&res).await {
                                return Err(err.into());
                            }
                        }
                        Err(_) => return Err("db response dropped".into()),
                    }
                }
                Command::Set { req_id, key, value } => {
                    let cmd = DbCommand::Set { key, value, req_id };
                    if db_channel.send(cmd).is_err() {
                        return Err("db manager dropped".into());
                    }
                }
            }
        }
    }
    println!("Connection closed");
    Ok(())
}
