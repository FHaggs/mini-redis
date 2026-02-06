import asyncio
import os
import random
import struct
import time
from multiprocessing import Process, Queue

HOST = "127.0.0.1"
PORT = 6379

# Tunables
PROCESSES = max(1, os.cpu_count() or 1)
CLIENTS_PER_PROCESS = 200
OPS_PER_CLIENT = 2000
GET_RATIO = 0.7  # 70% gets, 30% sets
KEYSPACE = 10_000
VALUE_SIZE = 64
TIMEOUT = 5.0


def build_set(key: bytes, value: bytes) -> bytes:
    return struct.pack(">BHI", 1, len(key), len(value)
    ) + key + value


def build_get(key: bytes) -> bytes:
    return struct.pack(">BH", 2, len(key)) + key


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

    for _ in range(OPS_PER_CLIENT):
        try:
            if random.random() < GET_RATIO:
                key = f"k{random.randint(0, KEYSPACE - 1)}".encode()
                writer.write(build_get(key))
                await writer.drain()
                data = await asyncio.wait_for(reader.read(4096), timeout=TIMEOUT)
                if data:
                    ok += 1
                else:
                    err += 1
            else:
                key = f"k{random.randint(0, KEYSPACE - 1)}".encode()
                value = os.urandom(VALUE_SIZE)
                writer.write(build_set(key, value))
                await writer.drain()
                ok += 1
        except Exception:
            err += 1
            break

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
