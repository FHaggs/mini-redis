import asyncio
import os
import random
import struct
import time
from multiprocessing import Process, Queue

HOST = "127.0.0.1"
PORT = 6379

MAGIC = 0x5244
VERSION = 1

OP_GET = 1
OP_SET = 2

STATUS_OK = 0
STATUS_NOT_FOUND = 1
STATUS_ERR = 2

# Tunables
PROCESSES = max(1, os.cpu_count() or 1)
CLIENTS_PER_PROCESS = 200
OPS_PER_CLIENT = 2000
GET_RATIO = 0.7  # 70% gets, 30% sets
KEYSPACE = 10_000
VALUE_SIZE = 64
TIMEOUT = 5.0


def build_request(opcode: int, req_id: int, payload: bytes) -> bytes:
    header = struct.pack(">HBBII", MAGIC, VERSION, opcode, req_id, len(payload))
    return header + payload


def build_set(key: bytes, value: bytes, req_id: int) -> bytes:
    payload = struct.pack(">H", len(key)) + key + struct.pack(">I", len(value)) + value
    return build_request(OP_SET, req_id, payload)


def build_get(key: bytes, req_id: int) -> bytes:
    payload = struct.pack(">H", len(key)) + key
    return build_request(OP_GET, req_id, payload)


async def recv_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    return await reader.readexactly(n)


async def parse_response(reader: asyncio.StreamReader) -> tuple[int, bytes | None]:
    header = await recv_exact(reader, 12)
    magic, version, status, _req_id, payload_len = struct.unpack("<HBBII", header)
    if magic != MAGIC or version != VERSION:
        raise ValueError("invalid response header")

    payload = await recv_exact(reader, payload_len) if payload_len > 0 else b""

    if status == STATUS_OK:
        if payload_len == 0:
            return status, None
        val_len = struct.unpack("<I", payload[:4])[0]
        return status, payload[4:4 + val_len]
    if status == STATUS_NOT_FOUND:
        return status, None
    if status == STATUS_ERR:
        return status, payload
    raise ValueError(f"unknown status: {status}")


async def client_task(client_id: int, results: Queue) -> None:
    ok = 0
    err = 0
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(HOST, PORT), timeout=TIMEOUT
        )
    except Exception:
        results.put((0, 1))
        return

    req_id = client_id * OPS_PER_CLIENT + 1
    for _ in range(OPS_PER_CLIENT):
        try:
            if random.random() < GET_RATIO:
                key = f"k{random.randint(0, KEYSPACE - 1)}".encode()
                writer.write(build_get(key, req_id))
                await writer.drain()
                status, _value = await asyncio.wait_for(
                    parse_response(reader), timeout=TIMEOUT
                )
                if status in (STATUS_OK, STATUS_NOT_FOUND):
                    ok += 1
                else:
                    err += 1
            else:
                key = f"k{random.randint(0, KEYSPACE - 1)}".encode()
                value = os.urandom(VALUE_SIZE)
                writer.write(build_set(key, value, req_id))
                await writer.drain()
                status, _value = await asyncio.wait_for(
                    parse_response(reader), timeout=TIMEOUT
                )
                if status == STATUS_OK:
                    ok += 1
                else:
                    err += 1
        except Exception:
            err += 1
            break
        finally:
            req_id += 1

    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass

    results.put((ok, err))


async def run_process(results: Queue) -> None:
    tasks = []
    for i in range(CLIENTS_PER_PROCESS):
        tasks.append(asyncio.create_task(client_task(i, results)))
    await asyncio.gather(*tasks)


def process_entry(results: Queue) -> None:
    asyncio.run(run_process(results))


def main() -> None:
    start = time.time()
    results = Queue()
    procs = []

    for _ in range(PROCESSES):
        p = Process(target=process_entry, args=(results,))
        p.start()
        procs.append(p)

    total_ok = 0
    total_err = 0

    expected = PROCESSES * CLIENTS_PER_PROCESS
    for _ in range(expected):
        ok, err = results.get()
        total_ok += ok
        total_err += err

    for p in procs:
        p.join()

    elapsed = time.time() - start
    total_ops = total_ok + total_err
    rps = total_ops / elapsed if elapsed > 0 else 0.0

    print("=== Stress Test Results ===")
    print(f"Processes: {PROCESSES}")
    print(f"Clients per process: {CLIENTS_PER_PROCESS}")
    print(f"Ops per client: {OPS_PER_CLIENT}")
    print(f"Total ops: {total_ops}")
    print(f"Success: {total_ok}")
    print(f"Errors: {total_err}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {rps:.2f} ops/sec")


if __name__ == "__main__":
    main()
