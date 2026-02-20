#!/usr/bin/env python3
"""
Latency-focused benchmark for mini-redis.
Measures p50, p95, p99 latency at different concurrency levels.
"""
import asyncio
import os
import struct
import time
import statistics
from dataclasses import dataclass
from typing import Optional

HOST = "127.0.0.1"
PORT = 6379

MAGIC = 0x5244
VERSION = 1

OP_GET = 1
OP_SET = 2
OP_DELETE = 3

STATUS_OK = 0
STATUS_NOT_FOUND = 1
STATUS_ERR = 2

# Benchmark parameters
WARMUP_OPS = 100
BENCHMARK_OPS = 5000
KEY_SIZE = 16
VALUE_SIZE = 64
KEYSPACE = 1000

# Test different concurrency levels
CONCURRENCY_LEVELS = [1, 10, 50, 100, 200]


@dataclass
class LatencyMeasurement:
    """Single request latency measurement"""
    req_id: int
    send_time: float
    recv_time: float
    op_type: str
    
    @property
    def latency_us(self) -> float:
        return (self.recv_time - self.send_time) * 1_000_000


def build_request(opcode: int, req_id: int, payload: bytes) -> bytes:
    """Build binary request frame"""
    header = struct.pack(">HBBII", MAGIC, VERSION, opcode, req_id, len(payload))
    return header + payload


def build_set(key: bytes, value: bytes, req_id: int) -> bytes:
    payload = struct.pack(">H", len(key)) + key + struct.pack(">I", len(value)) + value
    return build_request(OP_SET, req_id, payload)


def build_get(key: bytes, req_id: int) -> bytes:
    payload = struct.pack(">H", len(key)) + key
    return build_request(OP_GET, req_id, payload)


def build_delete(key: bytes, req_id: int) -> bytes:
    payload = struct.pack(">H", len(key)) + key
    return build_request(OP_DELETE, req_id, payload)


async def parse_response(reader: asyncio.StreamReader) -> tuple[int, int, Optional[bytes]]:
    """Parse response frame. Returns (status, req_id, value)"""
    header = await reader.readexactly(12)
    magic, version, status, req_id, payload_len = struct.unpack("<HBBII", header)
    
    if magic != MAGIC or version != VERSION:
        raise ValueError(f"invalid response header: magic={magic:04x}, version={version}")

    payload = await reader.readexactly(payload_len) if payload_len > 0 else b""

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


class LatencyBenchmark:
    """Single persistent connection for latency measurements"""
    
    def __init__(self, client_id: int):
        self.client_id = client_id
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.req_id = client_id * 1_000_000  # Unique ID space per client
        self.pending_requests: dict[int, tuple[float, str]] = {}
        self.measurements: list[LatencyMeasurement] = []
        
    async def connect(self):
        """Establish connection to server"""
        self.reader, self.writer = await asyncio.open_connection(HOST, PORT)
        
    async def close(self):
        """Close connection"""
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
    
    async def send_get(self, key: bytes) -> int:
        """Send GET request, return req_id"""
        req_id = self.req_id
        self.req_id += 1
        send_time = time.perf_counter()
        self.pending_requests[req_id] = (send_time, "GET")
        self.writer.write(build_get(key, req_id))
        await self.writer.drain()
        return req_id
    
    async def send_set(self, key: bytes, value: bytes) -> int:
        """Send SET request, return req_id"""
        req_id = self.req_id
        self.req_id += 1
        send_time = time.perf_counter()
        self.pending_requests[req_id] = (send_time, "SET")
        self.writer.write(build_set(key, value, req_id))
        await self.writer.drain()
        return req_id
    
    async def recv_response(self) -> LatencyMeasurement:
        """Receive one response and record latency"""
        status, req_id, _value = await parse_response(self.reader)
        recv_time = time.perf_counter()
        
        if req_id not in self.pending_requests:
            raise ValueError(f"unexpected req_id: {req_id}")
        
        send_time, op_type = self.pending_requests.pop(req_id)
        measurement = LatencyMeasurement(req_id, send_time, recv_time, op_type)
        
        if status == STATUS_ERR:
            raise RuntimeError(f"server error for req_id {req_id}")
        
        return measurement
    
    async def warmup(self, ops: int):
        """Warmup phase - populate keys and stabilize"""
        for i in range(ops):
            key = f"k{i % KEYSPACE}".encode()
            value = os.urandom(VALUE_SIZE)
            await self.send_set(key, value)
            await self.recv_response()
    
    async def run_sequential_benchmark(self, ops: int):
        """Run sequential requests (concurrency=1 per connection)"""
        self.measurements = []
        
        for i in range(ops):
            key = f"k{i % KEYSPACE}".encode()
            
            # 80% reads, 20% writes for realistic workload
            if i % 5 == 0:
                value = os.urandom(VALUE_SIZE)
                await self.send_set(key, value)
            else:
                await self.send_get(key)
            
            measurement = await self.recv_response()
            self.measurements.append(measurement)


async def run_concurrency_level(concurrency: int, ops_per_client: int):
    """Run benchmark at specific concurrency level"""
    print(f"\n--- Concurrency: {concurrency} ---")
    
    # Create clients
    clients = [LatencyBenchmark(i) for i in range(concurrency)]
    
    # Connect all
    await asyncio.gather(*[client.connect() for client in clients])
    
    # Warmup
    print("Warming up...")
    await asyncio.gather(*[client.warmup(WARMUP_OPS // concurrency) for client in clients])
    
    # Benchmark
    print(f"Running {ops_per_client * concurrency} operations...")
    start = time.perf_counter()
    await asyncio.gather(*[client.run_sequential_benchmark(ops_per_client) for client in clients])
    elapsed = time.perf_counter() - start
    
    # Close connections
    await asyncio.gather(*[client.close() for client in clients])
    
    # Collect all measurements
    all_measurements = []
    for client in clients:
        all_measurements.extend(client.measurements)
    
    # Calculate statistics
    latencies = [m.latency_us for m in all_measurements]
    latencies.sort()
    
    n = len(latencies)
    avg = statistics.mean(latencies)
    median = statistics.median(latencies)
    stdev = statistics.stdev(latencies) if n > 1 else 0
    
    p50 = latencies[int(n * 0.50)]
    p95 = latencies[int(n * 0.95)]
    p99 = latencies[int(n * 0.99)]
    p999 = latencies[min(int(n * 0.999), n - 1)]
    min_lat = min(latencies)
    max_lat = max(latencies)
    
    total_ops = len(all_measurements)
    throughput = total_ops / elapsed if elapsed > 0 else 0
    
    # Print results
    print(f"\nTotal operations: {total_ops:,}")
    print(f"Duration: {elapsed:.2f}s")
    print(f"Throughput: {throughput:,.0f} ops/sec")
    print(f"\nLatency (microseconds):")
    print(f"  min:    {min_lat:>8.0f} µs")
    print(f"  avg:    {avg:>8.0f} µs")
    print(f"  median: {median:>8.0f} µs")
    print(f"  stdev:  {stdev:>8.0f} µs")
    print(f"  p50:    {p50:>8.0f} µs")
    print(f"  p95:    {p95:>8.0f} µs")
    print(f"  p99:    {p99:>8.0f} µs")
    print(f"  p99.9:  {p999:>8.0f} µs")
    print(f"  max:    {max_lat:>8.0f} µs")
    
    return {
        "concurrency": concurrency,
        "throughput": throughput,
        "avg": avg,
        "p50": p50,
        "p95": p95,
        "p99": p99,
        "p999": p999,
    }


async def main():
    print("=" * 60)
    print("Mini-Redis Latency Benchmark")
    print("=" * 60)
    print(f"Keyspace: {KEYSPACE} keys")
    print(f"Value size: {VALUE_SIZE} bytes")
    print(f"Operations per test: {BENCHMARK_OPS}")
    print(f"Warmup operations: {WARMUP_OPS}")
    
    results = []
    
    for concurrency in CONCURRENCY_LEVELS:
        ops_per_client = BENCHMARK_OPS // concurrency
        try:
            result = await run_concurrency_level(concurrency, ops_per_client)
            results.append(result)
        except Exception as e:
            print(f"Error at concurrency {concurrency}: {e}")
            break
    
    # Summary table
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"{'Concurrency':<12} {'Throughput':<15} {'Avg':<10} {'p50':<10} {'p95':<10} {'p99':<10}")
    print("-" * 60)
    for r in results:
        print(f"{r['concurrency']:<12} {r['throughput']:>12,.0f} ops/s  "
              f"{r['avg']:>8.0f}µs {r['p50']:>8.0f}µs {r['p95']:>8.0f}µs {r['p99']:>8.0f}µs")


if __name__ == "__main__":
    asyncio.run(main())
