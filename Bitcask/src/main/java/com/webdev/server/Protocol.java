package com.webdev.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Wire protocol shared by the server and any client that talks to it.
 *
 * Every field is length-prefixed: a 4-byte big-endian length followed by
 * that many raw bytes. Keys and values in Bitcask are arbitrary byte[], so
 * this keeps the protocol binary-safe (no delimiter collisions like the
 * old \r\n-based framing had) and keeps strings and raw data framed the
 * same way.
 *
 * Request:
 *   [bytes: command name, UTF-8]
 *   GET:      [bytes: key]
 *   VIEW_ALL: (no further fields)
 *
 * Response:
 *   GET:      [1 byte status: 0 = not found, 1 = found]
 *             if found: [bytes: value]
 *   VIEW_ALL: [4-byte count N]
 *             N * ( [bytes: key] [bytes: value] )
 */
final class Protocol {

    static final byte NOT_FOUND = 0;
    static final byte FOUND = 1;

    private Protocol() {}

    static void writeBytes(DataOutputStream out, byte[] data) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new IOException("Negative length in protocol frame: " + length);
        }
        byte[] data = new byte[length];
        in.readFully(data);
        return data;
    }

    static void writeString(DataOutputStream out, String s) throws IOException {
        writeBytes(out, s.getBytes(StandardCharsets.UTF_8));
    }

    static String readString(DataInputStream in) throws IOException {
        return new String(readBytes(in), StandardCharsets.UTF_8);
    }
}