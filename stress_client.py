import asyncio
import os
import random
import struct
import time
from multiprocessing import Process, Queue
from dataclasses import dataclass

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
CLIENTS_PER_PROCESS = 50
OPS_PER_CLIENT = 200
GET_RATIO = 0.6  # 70% gets, 30% sets
KEYSPACE = 10_000
VALUE_SIZE = 64
TIMEOUT = 30.0
PIPELINE_SIZE = 10  # Send N requests before reading responses


@dataclass
class ClientResult:
    ok: int = 0
    err: int = 0
    latencies_us: list = None # type: ignore  # microseconds
    
    def __post_init__(self):
        if self.latencies_us is None:
            self.latencies_us = []


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


async def parse_response(reader: asyncio.StreamReader) -> tuple[int, int, bytes | None]:
    """Returns (status, req_id, value)"""
    header = await recv_exact(reader, 12)
    magic, version, status, req_id, payload_len = struct.unpack("<HBBII", header)
    if magic != MAGIC or version != VERSION:
        raise ValueError("invalid response header")

    payload = await recv_exact(reader, payload_len) if payload_len > 0 else b""

    if status == STATUS_OK:
        if payload_len == 0:
            return status, req_id, None
        val_len = struct.unpack("<I", payload[:4])[0]
        return status, req_id, payload[4:4 + val_len]
    if status == STATUS_NOT_FOUND:
        return status, req_id, None
    if status == STATUS_ERR:
        return status, req_id, payload
    raise ValueError(f"unknown status: {status}")


async def client_task(client_id: int, results: Queue) -> None:
    result = ClientResult()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(HOST, PORT), timeout=TIMEOUT
        )
    except Exception:
        results.put((0, 1, []))
        return

    req_id = client_id * OPS_PER_CLIENT + 1
    ops_remaining = OPS_PER_CLIENT
    
    try:
        while ops_remaining > 0:
            # Build a batch of requests
            batch_size = min(PIPELINE_SIZE, ops_remaining)
            batch_start_time = time.perf_counter()
            send_times = {}
            
            # Send batch without waiting for responses
            for _ in range(batch_size):
                send_times[req_id] = time.perf_counter()
                if random.random() < GET_RATIO:
                    key = f"k{random.randint(0, KEYSPACE - 1)}".encode()
                    writer.write(build_get(key, req_id))
                else:
                    key = f"k{random.randint(0, KEYSPACE - 1)}".encode()
                    value = os.urandom(VALUE_SIZE)
                    writer.write(build_set(key, value, req_id))
                req_id += 1
            
            # Flush once for entire batch
            await writer.drain()
            
            # Now read all responses
            for _ in range(batch_size):
                status, resp_req_id, _value = await asyncio.wait_for(
                    parse_response(reader), timeout=TIMEOUT
                )
                recv_time = time.perf_counter()
                
                if resp_req_id in send_times:
                    latency_us = (recv_time - send_times[resp_req_id]) * 1_000_000
                    result.latencies_us.append(latency_us)
                
                if status in (STATUS_OK, STATUS_NOT_FOUND):
                    result.ok += 1
                else:
                    result.err += 1
            
            ops_remaining -= batch_size
            
    except Exception as e:
        result.err += 1

    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass

    results.put((result.ok, result.err, result.latencies_us))


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
    all_latencies = []

    expected = PROCESSES * CLIENTS_PER_PROCESS
    for _ in range(expected):
        ok, err, latencies = results.get()
        total_ok += ok
        total_err += err
        all_latencies.extend(latencies)

    for p in procs:
        p.join()

    elapsed = time.time() - start
    total_ops = total_ok + total_err
    rps = total_ops / elapsed if elapsed > 0 else 0.0

    print("=== Stress Test Results ===")
    print(f"Processes: {PROCESSES}")
    print(f"Clients per process: {CLIENTS_PER_PROCESS}")
    print(f"Ops per client: {OPS_PER_CLIENT}")
    print(f"Pipeline size: {PIPELINE_SIZE}")
    print(f"Total ops: {total_ops}")
    print(f"Success: {total_ok}")
    print(f"Errors: {total_err}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {rps:,.0f} ops/sec")
    
    # Latency stats
    if all_latencies:
        all_latencies.sort()
        n = len(all_latencies)
        avg = sum(all_latencies) / n
        p50 = all_latencies[int(n * 0.50)]
        p90 = all_latencies[int(n * 0.90)]
        p99 = all_latencies[int(n * 0.99)]
        p999 = all_latencies[min(int(n * 0.999), n - 1)]
        
        print("\n=== Latency (client-side, includes RTT) ===")
        print(f"Avg:  {avg:,.0f} µs")
        print(f"p50:  {p50:,.0f} µs")
        print(f"p90:  {p90:,.0f} µs")
        print(f"p99:  {p99:,.0f} µs")
        print(f"p99.9: {p999:,.0f} µs")


if __name__ == "__main__":
    main()
