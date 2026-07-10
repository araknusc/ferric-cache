#!/usr/bin/env python3
import socket
import time
import struct

def send_command(host, port, command):
    """Send a command to the cache server and get response"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))

        # Send command
        command_bytes = command.encode('utf-8')
        sock.sendall(command_bytes)

        # Read response header (1 byte type + 4 bytes length)
        header = sock.recv(5)
        if len(header) < 5:
            return "Error: incomplete response"

        resp_type = header[0]
        length = struct.unpack('>I', header[1:5])[0]

        # Read response body
        response = b''
        while len(response) < length:
            chunk = sock.recv(length - len(response))
            if not chunk:
                break
            response += chunk

        return response.decode('utf-8', errors='replace')
    finally:
        sock.close()

print("Testing Master-Replica Replication")
print("=" * 50)

# Test 1: Write to master
print("\n1. Writing data to MASTER (port 7777)...")
response = send_command('127.0.0.1', 7777, 'SET testkey testvalue 0\n')
print(f"   Response: {response}")

# Wait a bit for replication
print("\n2. Waiting 1 second for replication...")
time.sleep(1)

# Test 2: Read from master
print("\n3. Reading from MASTER (port 7777)...")
response = send_command('127.0.0.1', 7777, 'GET testkey\n')
print(f"   Response: {response}")

# Test 3: Read from replica
print("\n4. Reading from REPLICA (port 7778)...")
response = send_command('127.0.0.1', 7778, 'GET testkey\n')
print(f"   Response: {response}")

# Test 4: Write another key to master
print("\n5. Writing another key to MASTER...")
response = send_command('127.0.0.1', 7777, 'SET anotherkey anothervalue 0\n')
print(f"   Response: {response}")

time.sleep(1)

# Test 5: Read from replica
print("\n6. Reading second key from REPLICA...")
response = send_command('127.0.0.1', 7778, 'GET anotherkey\n')
print(f"   Response: {response}")

# Test 6: Delete from master
print("\n7. Deleting key from MASTER...")
response = send_command('127.0.0.1', 7777, 'DELETE testkey\n')
print(f"   Response: {response}")

time.sleep(1)

# Test 7: Verify deletion replicated
print("\n8. Verifying deletion on REPLICA...")
response = send_command('127.0.0.1', 7778, 'GET testkey\n')
print(f"   Response: {response}")

print("\n" + "=" * 50)
print("Replication test complete!")
