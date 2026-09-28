#!/usr/bin/env python3
"""
Bitcask client.

Talks to BitcaskServer over the length-prefixed binary protocol described
in Protocol.java:

Request:
    [bytes: command name, UTF-8]
    GET:      [bytes: key]
    VIEW_ALL: (nothing further)

Response:
    GET:      [1 byte status: 0 = not found, 1 = found]
              if found: [bytes: value]
    VIEW_ALL: [4-byte count N]
              N * ( [bytes: key] [bytes: value] )

Every "[bytes: ...]" field is a 4-byte big-endian length followed by that
many raw bytes.
"""

import argparse
import csv
import socket
import struct
import sys
import threading
import time

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9090


class ProtocolError(Exception):
    pass


def recv_exact(sock, n):
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ProtocolError("Connection closed by server while reading response")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def write_bytes(sock, data):
    sock.sendall(struct.pack(">I", len(data)) + data)


def read_bytes(sock):
    (length,) = struct.unpack(">I", recv_exact(sock, 4))
    return recv_exact(sock, length)


def write_string(sock, s):
    write_bytes(sock, s.encode("utf-8"))


def connect(host, port):
    return socket.create_connection((host, port))


def cmd_get(host, port, key):
    with connect(host, port) as sock:
        write_string(sock, "GET")
        write_bytes(sock, key.encode("utf-8"))

        status = recv_exact(sock, 1)[0]
        if status == 0:
            return None
        return read_bytes(sock)


def cmd_view_all(host, port):
    """Returns a list of (key_bytes, value_bytes) tuples."""
    with connect(host, port) as sock:
        write_string(sock, "VIEW_ALL")

        (count,) = struct.unpack(">I", recv_exact(sock, 4))
        entries = []
        for _ in range(count):
            key = read_bytes(sock)
            value = read_bytes(sock)
            entries.append((key, value))
        return entries


def decode(b):
    """Best-effort text decoding for display/CSV; falls back to a repr for
    values that aren't valid UTF-8 so the CSV still gets written."""
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return repr(b)


def write_csv(path, entries):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["key", "value"])
        for key, value in entries:
            writer.writerow([decode(key), decode(value)])


def do_view_all(args):
    entries = cmd_view_all(args.host, args.port)
    filename = f"{int(time.time())}.csv"
    write_csv(filename, entries)
    print(f"Wrote {len(entries)} entries to {filename}")


def do_view(args):
    if args.key is None:
        print("--view requires --key=SOME_KEY", file=sys.stderr)
        sys.exit(1)

    value = cmd_get(args.host, args.port, args.key)
    if value is None:
        print(f"Key not found: {args.key}", file=sys.stderr)
        sys.exit(1)

    sys.stdout.buffer.write(value)
    if not value.endswith(b"\n"):
        sys.stdout.write("\n")


def do_perf(args):
    if args.clients is None or args.clients < 1:
        print("--perf requires --clients=N (N >= 1)", file=sys.stderr)
        sys.exit(1)

    timestamp = int(time.time())
    errors = []

    def worker(thread_num):
        try:
            entries = cmd_view_all(args.host, args.port)
            filename = f"{timestamp}_thread_{thread_num}.csv"
            write_csv(filename, entries)
        except Exception as e:
            errors.append((thread_num, e))

    threads = [
        threading.Thread(target=worker, args=(i,))
        for i in range(1, args.clients + 1)
    ]

    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    print(f"{args.clients} clients finished in {elapsed:.3f}s")
    if errors:
        print(f"{len(errors)} client(s) failed:", file=sys.stderr)
        for thread_num, e in errors:
            print(f"  thread {thread_num}: {e}", file=sys.stderr)
        sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="Bitcask TCP client")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--view-all", action="store_true")
    mode.add_argument("--view", action="store_true")
    mode.add_argument("--perf", action="store_true")

    parser.add_argument("--key")
    parser.add_argument("--clients", type=int)

    return parser.parse_args()


def main():
    args = parse_args()

    try:
        if args.view_all:
            do_view_all(args)
        elif args.view:
            do_view(args)
        elif args.perf:
            do_perf(args)
    except (ConnectionError, ProtocolError, OSError) as e:
        print(f"Connection error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()