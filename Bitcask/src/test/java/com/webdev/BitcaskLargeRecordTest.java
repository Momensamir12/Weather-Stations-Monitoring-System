package com.webdev;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Stress test focused on RECORD SIZE, not just record count. testManyKeysLoop
 * (in BitcaskLoopTest) already covers 1000 keys, but every value there is a
 * fixed 16 random chars -- it never exercises variable-length keys, values
 * bigger than the size-rotation threshold, or binary (non-UTF-8) key bytes.
 * This harness targets exactly those gaps:
 *
 *  - key sizes uniformly random in [MIN_KEY_SIZE, MAX_KEY_SIZE]
 *  - value sizes uniformly random in [MIN_VALUE_SIZE, MAX_VALUE_SIZE],
 *    intentionally reaching well past the 512-byte rotation threshold so
 *    many records force a file rotation by themselves
 *  - a handful of explicit edge sizes (1-byte key, 1-byte value, and a
 *    single oversized record) mixed in among the random ones
 *  - raw random bytes for keys (not text), to make sure nothing in the
 *    read/write/scan path assumes printable/UTF-8 content
 *  - writes happen against one Bitcask instance, then that instance is
 *    closed and a brand-new one is opened before reading, so verification
 *    goes through buildKeyDirectory()'s full-file-scan path (the code
 *    we've been debugging) rather than the in-memory state left over
 *    from put()
 *  - reads happen in a shuffled order against an in-memory reference map
 */
public class BitcaskLargeRecordTest {

    private static final String DIR = "Bitcask/bitcask-files/";
    private static final int RECORD_COUNT = 1000;

    private static final int MIN_KEY_SIZE = 4;
    private static final int MAX_KEY_SIZE = 300;
    private static final int MIN_VALUE_SIZE = 8;
    private static final int MAX_VALUE_SIZE = 2000; // well past the 512-byte rotation threshold

    public static void main(String[] args) throws Exception {
        cleanDirectory();

        Random random = new Random(1234); // fixed seed -> reproducible failures
        Map<KeyWrapper, byte[]> reference = new HashMap<>();

        System.out.println("== writing " + RECORD_COUNT + " records with variable key/value sizes ==");
        Bitcask writer = new Bitcask();

        for (int i = 0; i < RECORD_COUNT; i++) {
            byte[] key = randomBytes(random, MIN_KEY_SIZE, MAX_KEY_SIZE);
            byte[] value = randomBytes(random, MIN_VALUE_SIZE, MAX_VALUE_SIZE);
            writer.put(key, value);
            reference.put(new KeyWrapper(key), value);
        }

        // Explicit edge cases, mixed in on top of the random records above.
        putEdgeCase(writer, reference, random, "1-byte key", 1, 200);
        putEdgeCase(writer, reference, random, "1-byte value", 200, 1);
        putEdgeCase(writer, reference, random, "oversized single record", 500, 8000);

        writer.close();

        System.out.println("---- files on disk ----");
        long fileCount = countFiles();
        System.out.println(fileCount + " data files written");

        // Fresh instance -> forces buildKeyDirectory() to scan every file from disk,
        // exercising the exact header/key-read/skip-value loop this conversation has
        // been about, now against realistic variable-size, binary-safe records.
        System.out.println("---- reopening (forces full disk scan) ----");
        Bitcask reader = new Bitcask();

        System.out.println("---- verifying " + reference.size() + " records in shuffled order ----");
        List<KeyWrapper> keys = new ArrayList<>(reference.keySet());
        Collections.shuffle(keys, random);

        int failures = 0;
        for (KeyWrapper kw : keys) {
            byte[] expected = reference.get(kw);
            byte[] actual = reader.get(kw.bytes);
            if (!Arrays.equals(expected, actual)) {
                failures++;
                System.out.printf("FAIL key(len=%d)=%s expectedLen=%d actualLen=%d%n",
                        kw.bytes.length, preview(kw.bytes),
                        expected.length, actual == null ? -1 : actual.length);
            }
        }

        reader.close();

        System.out.println();
        System.out.println(reference.size() + " records verified, " + failures + " mismatches");
        System.out.println(failures == 0 ? "ALL TESTS PASSED" : "SOME TESTS FAILED");
    }

    private static void putEdgeCase(Bitcask bitcask, Map<KeyWrapper, byte[]> reference,
                                     Random random, String label, int keySize, int valueSize) throws Exception {
        byte[] key = randomBytes(random, keySize, keySize);
        byte[] value = randomBytes(random, valueSize, valueSize);
        bitcask.put(key, value);
        reference.put(new KeyWrapper(key), value);
        System.out.printf("edge case [%s]: keySize=%d valueSize=%d%n", label, keySize, valueSize);
    }

    private static byte[] randomBytes(Random random, int minLen, int maxLen) {
        int len = minLen == maxLen ? minLen : minLen + random.nextInt(maxLen - minLen + 1);
        byte[] bytes = new byte[len];
        random.nextBytes(bytes); // raw bytes, not text -- exercises binary-safety
        return bytes;
    }

    private static String preview(byte[] bytes) {
        int n = Math.min(8, bytes.length);
        StringBuilder sb = new StringBuilder("0x");
        for (int i = 0; i < n; i++) sb.append(String.format("%02x", bytes[i]));
        if (bytes.length > n) sb.append("...");
        return sb.toString();
    }

    private static void cleanDirectory() throws IOException {
        Path dir = Path.of(DIR);
        Files.createDirectories(dir);
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(p -> p.toFile().delete());
        }
    }

    private static long countFiles() throws IOException {
        try (Stream<Path> stream = Files.list(Path.of(DIR))) {
            return stream.count();
        }
    }

    /** byte[] has no natural equals/hashCode, so wrap it for use as a HashMap key
     *  (separate from Bitcask's own ByteArrayKey -- this is just test-side bookkeeping). */
    private static final class KeyWrapper {
        final byte[] bytes;
        KeyWrapper(byte[] bytes) { this.bytes = bytes; }

        @Override
        public boolean equals(Object o) {
            return o instanceof KeyWrapper && Arrays.equals(bytes, ((KeyWrapper) o).bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}