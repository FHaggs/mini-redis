use bytes::{Buf, Bytes, BytesMut};

#[derive(Debug)]
pub enum Command {
    Set(Bytes, Bytes),
    Get(Bytes),
}

pub fn try_parse(frame: &mut BytesMut) -> Option<Command> {
    if frame.len() < 1 {
        return None; // wait for more data
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
    let val_len = u32::from_be_bytes([frame[3], frame[4], frame[5], frame[6]]) as usize;

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

#[cfg(test)]
mod tests {
    use super::{try_parse, Command};

    #[test]
    fn test_parse_set() {
        let buff = &mut bytes::BytesMut::from(&[
            1u8,                   // cmd = SET
            0, 3,                 // key_len = 3
            0, 0, 0, 5,          // val_len = 5
            b'k', b'e', b'y',     // key = "key"
            b'v', b'a', b'l', b'u', b'e' // value = "value"
        ][..]);
        let command = try_parse(buff).unwrap();
        match command {
            Command::Set(key, value) => {
                assert_eq!(&key[..], b"key");
                assert_eq!(&value[..], b"value");
            }
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
        let command = try_parse(buff).unwrap();
        match command {
            Command::Get(key) => assert_eq!(&key[..], b"key"),
            _ => panic!("Expected GET"),
        }
    }
}
