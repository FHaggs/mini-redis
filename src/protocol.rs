use bytes::{Buf, BufMut, Bytes, BytesMut};

const MAGIC: u16 = 0x5244; // "RD"
const VERSION: u8 = 1;
const HEADER_LEN: usize = 12;
pub const MAX_FRAME: usize = 1024 * 1024; // 1MB, tune as needed


#[derive(Debug, Clone, Copy)]
pub enum OpCode {
    Get = 1,
    Set = 2,
    Del = 3,
}

#[derive(Debug, Clone, Copy)]
pub enum Status {
    Ok = 0,
    NotFound = 1,
    Err = 2,
}

#[derive(Debug, Clone, Copy)]
pub struct RequestFrameHeader {
    pub opcode: OpCode,
    pub req_id: u32,
    pub payload_len: u32,
}
#[derive(Debug, Clone, Copy)]
pub struct ResponseFrameHeader {
    pub magic: u16,
    pub version: u8,
    pub status: Status,
    pub req_id: u32,
    pub payload_len: u32,
}

#[derive(Debug)]
pub enum Command {
    Get { req_id: u32, key: bytes::Bytes },
    Set { req_id: u32, key: bytes::Bytes, value: bytes::Bytes },
    Delete { req_id: u32, key: bytes::Bytes },
}

#[derive(Debug)]
pub enum Response {
    Ok { req_id: u32, value: Option<bytes::Bytes> },
    NotFound { req_id: u32 },
    Err { req_id: u32, message: bytes::Bytes },
}

/*
REQUEST HEADER
Header 12 Bytes:
magic   : u16  (0x5244 = "RD")
version : u8   (1)
opcode  : u8   (GET=1, SET=2)
req_id  : u32  (request id)
payload_len : u32 (length of payload in bytes)
Request payloads:
RESPONSE HEADER
Header 12 Bytes:
magic   : u16  (0x5244 = "RD")
version : u8   (1)
status  : u8   (OK=0, NOT_FOUND=1, ERR=2)
req_id  : u32  (request id)
payload_len : u32 (length of payload in bytes)


GET: [key:_len:u16][key]
SET: [key_len:u16][key][val_len:u32][val]
Response payloads:

OK + GET: [val_len:u32][val]
OK + SET: empty
NOT_FOUND: empty
ERR: [msg_len:u16][msg]
*/

enum ParseState {
    Header,
    Payload(RequestFrameHeader),
}

pub struct Frame {
    state: ParseState,
}
impl Frame {
    pub fn new() -> Self {
        Self {
            state: ParseState::Header,
        }
    }
    pub fn try_parse(&mut self, buf: &mut BytesMut) -> Option<Command> {
        loop {
            match self.state {
                ParseState::Header => {
                    if let Some(header) = parse_req_header(buf) {
                        self.state = ParseState::Payload(header);
                        continue;
                    }
                    return None;
                }
                ParseState::Payload(header) => {
                    if buf.len() < header.payload_len as usize {
                        return None; // wait for more data
                    }
                    let cmd = match header.opcode {
                        OpCode::Get => parse_get_command(buf, &header),
                        OpCode::Set => parse_set_command(buf, &header),
                        OpCode::Del => parse_del_command(buf, &header)
                    };
                    self.state = ParseState::Header;
                    return cmd;
                }
            }
        }
    }
}

fn parse_del_command(buf: &mut BytesMut, header: &RequestFrameHeader) -> Option<Command> {
    let key_len = buf.get_u16() as usize;
    if buf.len() < key_len {
        return None;
    }
    let key = buf.split_to(key_len).freeze();
    Some(Command::Delete {
        req_id: header.req_id,
        key,
    })
}

fn parse_get_command(buf: &mut BytesMut, header: &RequestFrameHeader) -> Option<Command> {
    let key_len = buf.get_u16() as usize;
    if buf.len() < key_len {
        return None;
    }
    let key = buf.split_to(key_len).freeze();
    Some(Command::Get {
        req_id: header.req_id,
        key,
    })
}
fn parse_set_command(buf: &mut BytesMut, header: &RequestFrameHeader) -> Option<Command> {
    let key_len = buf.get_u16() as usize;
    if buf.len() < key_len {
        return None;
    }
    let key = buf.split_to(key_len).freeze();
    if buf.len() < 4 {
        return None;
    }
    let val_len = buf.get_u32() as usize;
    if buf.len() < val_len {
        return None;
    }
    let value = buf.split_to(val_len).freeze();
    Some(Command::Set {
        req_id: header.req_id,
        key,
        value,
    })
}

pub fn parse_req_header(frame: &mut BytesMut) -> Option<RequestFrameHeader> {
    if frame.len() < HEADER_LEN {
        return None;
    }
    let magic = frame.get_u16();
    if magic != MAGIC {
        eprintln!("Invalid magic: {magic}");
        return None;
    }
    let version = frame.get_u8();
    if version != VERSION {
        eprintln!("Unsupported version: {version}");
        return None;
    }
    let opcode = match frame.get_u8() {
        1 => OpCode::Get,
        2 => OpCode::Set,
        3 => OpCode::Del,
        other => {
            eprintln!("Invalid opcode: {other}");
            return None;
        }
    };
    let req_id = frame.get_u32();
    let payload_len = frame.get_u32();
    Some(RequestFrameHeader {
        opcode,
        req_id,
        payload_len,
    })
}

fn create_response_header(status: Status, req_id: u32, payload_len: u32) -> ResponseFrameHeader {
    ResponseFrameHeader {
        magic: MAGIC,
        version: VERSION,
        status,
        req_id,
        payload_len,
    }
}

pub fn encode_response(response: Response) -> Bytes {
    match response {
        Response::Ok { req_id, value } => {
            let payload_len = match &value {
                Some(v) => 4 + v.len() as u32,
                None => 0,
            };
            let header = create_response_header(Status::Ok, req_id, payload_len);
            let mut buf = BytesMut::with_capacity(HEADER_LEN + payload_len as usize);
            buf.put_u16_le(header.magic);
            buf.put_u8(header.version);
            buf.put_u8(header.status as u8);
            buf.put_u32_le(header.req_id);
            buf.put_u32_le(header.payload_len);
            if let Some(v) = value {
                buf.put_u32_le(v.len() as u32);
                buf.extend_from_slice(&v);
            }
            buf.freeze()
        }
        Response::NotFound { req_id } => {
            let header = create_response_header(Status::NotFound, req_id, 0);
            let mut buf = BytesMut::with_capacity(HEADER_LEN);
            buf.put_u16_le(header.magic);
            buf.put_u8(header.version);
            buf.put_u8(header.status as u8);
            buf.put_u32_le(header.req_id);
            buf.put_u32_le(header.payload_len);
            buf.freeze()
        }
        Response::Err { req_id, message } => {
            let msg_len = message.len() as u16;
            let payload_len = 2 + msg_len as u32;
            let header = create_response_header(Status::Err, req_id, payload_len);
            let mut buf = BytesMut::with_capacity(HEADER_LEN + payload_len as usize);
            buf.put_u16_le(header.magic);
            buf.put_u8(header.version);
            buf.put_u8(header.status as u8);
            buf.put_u32_le(header.req_id);
            buf.put_u32_le(header.payload_len);
            buf.put_u16_le(msg_len);
            buf.extend_from_slice(&message);
            buf.freeze()
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn parse_get_full_frame() {
        let mut buf = BytesMut::new();
        let key = b"foo";
        let payload_len = 2 + key.len() as u32;

        // header
        buf.put_u16(MAGIC);
        buf.put_u8(VERSION);
        buf.put_u8(OpCode::Get as u8);
        buf.put_u32(42);
        buf.put_u32(payload_len);

        // payload
        buf.put_u16(key.len() as u16);
        buf.extend_from_slice(key);

        let mut frame = Frame::new();
        let cmd = frame.try_parse(&mut buf).expect("cmd");

        match cmd {
            Command::Get { req_id, key } => {
                assert_eq!(req_id, 42);
                assert_eq!(&key[..], b"foo");
            }
            _ => panic!("expected GET"),
        }
    }

    #[test]
    fn parse_set_full_frame() {
        let mut buf = BytesMut::new();
        let key = b"foo";
        let val = b"bar";
        let payload_len = 2 + key.len() as u32 + 4 + val.len() as u32;

        // header
        buf.put_u16(MAGIC);
        buf.put_u8(VERSION);
        buf.put_u8(OpCode::Set as u8);
        buf.put_u32(7);
        buf.put_u32(payload_len);

        // payload
        buf.put_u16(key.len() as u16);
        buf.extend_from_slice(key);
        buf.put_u32(val.len() as u32);
        buf.extend_from_slice(val);

        let mut frame = Frame::new();
        let cmd = frame.try_parse(&mut buf).expect("cmd");

        match cmd {
            Command::Set { req_id, key, value } => {
                assert_eq!(req_id, 7);
                assert_eq!(&key[..], b"foo");
                assert_eq!(&value[..], b"bar");
            }
            _ => panic!("expected SET"),
        }
    }

    #[test]
    fn parse_get_incremental() {
        let mut buf = BytesMut::new();
        let key = b"abc";
        let payload_len = 2 + key.len() as u32;

        // header only
        buf.put_u16(MAGIC);
        buf.put_u8(VERSION);
        buf.put_u8(OpCode::Get as u8);
        buf.put_u32(99);
        buf.put_u32(payload_len);

        let mut frame = Frame::new();
        assert!(frame.try_parse(&mut buf).is_none());

        // payload arrives later
        buf.put_u16(key.len() as u16);
        buf.extend_from_slice(key);

        let cmd = frame.try_parse(&mut buf).expect("cmd");
        match cmd {
            Command::Get { req_id, key } => {
                assert_eq!(req_id, 99);
                assert_eq!(&key[..], b"abc");
            }
            _ => panic!("expected GET"),
        }
    }
}
