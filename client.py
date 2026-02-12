import socket
import struct

MAGIC = 0x5244
VERSION = 1

OP_GET = 1
OP_SET = 2
OP_DEL = 3

STATUS_OK = 0
STATUS_NOT_FOUND = 1
STATUS_ERR = 2


def recv_exact(sock: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("socket closed")
        data.extend(chunk)
    return bytes(data)


def build_request(opcode: int, req_id: int, payload: bytes) -> bytes:
    header = struct.pack(">HBBII", MAGIC, VERSION, opcode, req_id, len(payload))
    return header + payload


def parse_response(sock: socket.socket) -> bytes | None:
    header = recv_exact(sock, 12)
    magic, version, status, req_id, payload_len = struct.unpack("<HBBII", header)
    if magic != MAGIC or version != VERSION:
        raise ValueError("invalid response header")

    payload = recv_exact(sock, payload_len) if payload_len > 0 else b""

    if status == STATUS_OK:
        if payload_len == 0:
            return None
        val_len = struct.unpack("<I", payload[:4])[0]
        return payload[4:4 + val_len]
    if status == STATUS_NOT_FOUND:
        return None
    if status == STATUS_ERR:
        return payload
    raise ValueError(f"unknown status: {status}")


req_id = 1

while True:
    sock = socket.create_connection(("127.0.0.1", 6379))
    cmd = input("cmd (set/get): ").strip().split()
    if not cmd:
        sock.close()
        continue

    if cmd[0] == "set":
        k = cmd[1].encode()
        v = cmd[2].encode()
        payload = struct.pack(">H", len(k)) + k + struct.pack(">I", len(v)) + v
        pkt = build_request(OP_SET, req_id, payload)
        sock.sendall(pkt)
        print(f"SET sent (req_id={req_id})")
        response = parse_response(sock)
        if response is None:
            print("Response empty")
        else:
            print(f"Value: {response.decode(errors='replace')}")

    elif cmd[0] == "get":
        k = cmd[1].encode()
        payload = struct.pack(">H", len(k)) + k
        pkt = build_request(OP_GET, req_id, payload)
        sock.sendall(pkt)

        response = parse_response(sock)
        if response is None:
            print("Key not found or empty")
        else:
            print(f"Value: {response.decode(errors='replace')}")
    elif cmd[0] == "del":
        k = cmd[1].encode()
        payload = struct.pack(">H", len(k)) + k
        pkt = build_request(OP_DEL, req_id, payload)
        sock.sendall(pkt)

        response = parse_response(sock)
        if response is None:
            print("Key not found or empty")
        else:
            print(f"Value: {response.decode(errors='replace')}")

    

    req_id += 1
    sock.close()