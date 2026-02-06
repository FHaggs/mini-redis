use std::collections::HashMap;

use bytes::{Buf, BytesMut, Bytes};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::mpsc};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;


type Db = HashMap<Bytes, Bytes>;

#[derive(Debug)]
enum DbCommand {
    Set { key: Bytes, value: Bytes },
    Get { key: Bytes, tx: tokio::sync::oneshot::Sender<Option<Bytes>> },
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    println!("Server listening on {}", addr);

    let (db_tx, db_rx) = mpsc::unbounded_channel::<DbCommand>();
    let db = HashMap::new();
    tokio::spawn(db_manager(db_rx, db));

    loop {
        let (socket, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                eprintln!("accept error: {err}");
                continue;
            }
        };
        let db_tx = db_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = process(socket, db_tx).await {
                eprintln!("connection error: {err}");
            }
        });
    }
}

async fn db_manager(mut db_rx: mpsc::UnboundedReceiver<DbCommand>, mut db: Db) {
    println!("DB manager started");
    while let Some(command) = db_rx.recv().await {
        match command {
            DbCommand::Get { key, tx } => {
                print!("DB GET: {:?} => ", key);
                let res = db.get(&key)
                    .map(|v| v.clone());
                let _ = tx.send(res);
            }
            DbCommand::Set { key, value } => {
                print!("DB SET: {:?} => {:?}\n", key, value);
                db.insert(key, value);
            }
        }
    }
}

async fn process(mut socket: TcpStream, db_channel: mpsc::UnboundedSender<DbCommand>) -> Result<()> {
    let mut buf = BytesMut::with_capacity(4096);
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

        while let Some(command) = try_parse(&mut buf) {
            match command {
                Command::Get(key) =>{
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let cmd = DbCommand::Get { key, tx };
                    if db_channel.send(cmd).is_err() {
                        return Err("db manager dropped".into());
                    }
                    match rx.await {
                        Ok(res) => {
                            if let Some(value) = res {
                                if let Err(err) = socket.write_all(&value).await {
                                    return Err(err.into());
                                }
                            } else if let Err(err) = socket.write_all(b"NOT_FOUND").await {
                                return Err(err.into());
                            }
                        }
                        Err(_) => return Err("db response dropped".into()),
                    }
                }
                Command::Set(key, value) => {
                    let cmd = DbCommand::Set { key, value };
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


#[derive(Debug)]
enum Command {
    Set(Bytes, Bytes),
    Get(Bytes),
}


fn try_parse(frame: &mut BytesMut) -> Option<Command> {
    if frame.len() < 1 {
        return None;
    }

    let cmd = frame[0];

    match cmd {
        1 => parse_set(frame),
        2 => parse_get(frame),
        _ => {
            frame.clear(); // protocol violation
            None
        }
    }
}

/*
PROTOCOL
SET:
[cmd: u8=1][key_len: u16][val_len: u32][key][value]

GET:
[cmd: u8=2][key_len: u16][key]
*/
fn parse_set(frame: &mut BytesMut) -> Option<Command> {
    if frame.len() < 1 + 2 + 4 {
        return None;
    }

    let key_len = u16::from_be_bytes([frame[1], frame[2]]) as usize;
    let val_len = u32::from_be_bytes([
        frame[3], frame[4], frame[5], frame[6]
    ]) as usize;

    let total = 1 + 2 + 4 + key_len + val_len;

    if frame.len() < total {
        return None;
    }

    frame.advance(7); // cmd + lengths

    let key = frame.split_to(key_len).freeze();
    let val = frame.split_to(val_len).freeze();

    Some(Command::Set(key, val))
}


fn parse_get(frame: &mut BytesMut) -> Option<Command> {
    if frame.len() < 1 + 2 {
        return None;
    }

    let key_len = u16::from_be_bytes([frame[1], frame[2]]) as usize;

    let total = 1 + 2 + key_len;

    if frame.len() < total {
        return None;
    }

    frame.advance(3);

    let key = frame.split_to(key_len).freeze();

    Some(Command::Get(key))
}


mod tests {

    #[test]
    fn test_parse_set() {
        let buff = &mut bytes::BytesMut::from(&[
            1u8,                   // cmd = SET
            0, 3,                 // key_len = 3
            0, 0, 0, 5,          // val_len = 5
            b'k', b'e', b'y',     // key = "key"
            b'v', b'a', b'l', b'u', b'e' // value = "value"
        ][..]);
        let command = super::try_parse(buff).unwrap();
        match command {
            super::Command::Set(key, value) => {
                assert_eq!(&key[..], b"key");
                assert_eq!(&value[..], b"value");
            },
            _ => panic!("Expected SET command"),
        }
    }

    #[test]
    fn test_parse_get() {
        let buff = &mut bytes::BytesMut::from(&[
            2u8,
            0, 3,
            b'k', b'e', b'y' 
        ][..]);
        let command = super::try_parse(buff).unwrap();
        match command {
            super::Command::Get(key) => assert_eq!(&key[..], b"key"),
            _ => panic!("Expected GEt"),
        }
    }
}