package com.webdev;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Deterministic repro for the file-scan-ordering bug: writes the SAME key ("A")
 * into 11 raw data files (fileId 1..11), each with a distinguishable value.
 * File 11 is the "correct" answer, since it's the highest fileId = the most
 * recently written record. This bypasses Bitcask.put()/rotation entirely so we
 * don't need thousands of filler writes to reach a double-digit fileId.
 */
public class BitcaskOrderingBugTest {

    private static final String DIR = "Bitcask/bitcask-files/";
    private static final int NUM_FILES = 11; // must exceed 9 to force a double-digit fileId

    public static void main(String[] args) throws Exception {
        cleanDirectory();

        for (int fileId = 1; fileId <= NUM_FILES; fileId++) {
            writeRawRecord(fileId, "A", "value-" + fileId);
        }

        System.out.println("---- constructing Bitcask (scans + prints keyDir) ----");
        Bitcask bitcask = new Bitcask();

        byte[] raw = bitcask.get("A".getBytes(StandardCharsets.UTF_8));
        String actual = raw == null ? null : new String(raw, StandardCharsets.UTF_8);
        String expected = "value-" + NUM_FILES; // fileId 11 = written last = should win

        System.out.println("---- verifying ----");
        System.out.printf("%s key=A expected=%s actual=%s%n",
                expected.equals(actual) ? "PASS" : "FAIL", expected, actual);

        bitcask.close();
    }

    /** Builds a single raw record matching Bitcask.put()'s exact on-disk layout:
     *  [keySize:int][valueSize:int][key bytes][value bytes], and writes it as
     *  the entire contents of fileId.data. */
    private static void writeRawRecord(int fileId, String key, String value) throws IOException {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        byte[] v = value.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + k.length + v.length);
        buf.putInt(k.length);
        buf.putInt(v.length);
        buf.put(k);
        buf.put(v);

        Files.write(Path.of(DIR + fileId + ".data"), buf.array());
    }

    private static void cleanDirectory() throws IOException {
        Path dir = Path.of(DIR);
        Files.createDirectories(dir);
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(p -> p.toFile().delete());
        }
    }
}