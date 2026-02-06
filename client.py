import socket
import struct

while True:
    sock = socket.create_connection(("127.0.0.1", 6379))
    cmd = input("cmd (set/get): ").strip().split()

    if cmd[0] == "set":
        k = cmd[1].encode()
        v = cmd[2].encode()
        pkt = struct.pack(">BHI", 1, len(k), len(v)) + k + v
        sock.sendall(pkt)
        print("SET sent")

    elif cmd[0] == "get":
        k = cmd[1].encode()
        pkt = struct.pack(">BH", 2, len(k)) + k
        sock.sendall(pkt)
        
        response = sock.recv(4096)
        if response:
            print(f"Value: {response.decode()}")
        else:
            print("Key not found or error")

    sock.close()