use bytes::{Buf, Bytes, BytesMut};

const MAGIC: u16 = 0x5244; // "RD"
const VERSION: u8 = 1;
const HEADER_LEN: usize = 12;
const LEN_PREFIX: usize = 4;
pub const MAX_FRAME: usize = 1024 * 1024; // 1MB, tune as needed


#[derive(Debug, Clone, Copy)]
pub enum OpCode {
    Get = 1,
    Set = 2,
}

#[derive(Debug, Clone, Copy)]
pub enum Status {
    Ok = 0,
    NotFound = 1,
    Err = 2,
}

#[derive(Debug)]
pub struct RequestFrameHeader {
    pub magic: u16,
    pub version: u8,
    pub opcode: OpCode,
    pub req_id: u32,
    pub payload_len: u32,
}
#[derive(Debug)]
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


GET: [key]
SET: [key_len:u16][key][val_len:u32][val]
Response payloads:

OK + GET: [val_len:u32][val]
OK + SET: empty
NOT_FOUND: empty
ERR: [msg_len:u16][msg]
*/


pub fn try_parse(frame: &mut BytesMut) -> Option<Command> {
    if frame.len() < 2 {
        return None; // wait for more data
    }

    let magic = frame.get_u16();
    if magic != MAGIC {
        eprintln!("Invalid magic: {magic}");
        return None;
    }


    None
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
        other => {
            eprintln!("Invalid opcode: {other}");
            return None;
        }
    };
    let req_id = frame.get_u32();
    let payload_len = frame.get_u32();
    Some(RequestFrameHeader {
        magic,
        version,
        opcode,
        req_id,
        payload_len,
    })
}

