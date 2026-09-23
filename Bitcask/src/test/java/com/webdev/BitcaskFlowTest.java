package com.webdev;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class BitcaskFlowTest {

    private static final Path DATA_DIR = Path.of("Bitcask/bitcask-files/");
    private static final int VALUE_SIZE = 50_000;

    private Bitcask bitcask;

    @BeforeEach
    void setUp() throws IOException {
        cleanDataDir();
        bitcask = new Bitcask();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (bitcask != null) {
            bitcask.close();
        }
        cleanDataDir();
    }

    /**
     * Full flow: insert enough keys to force a file rollover, overwrite a
     * subset of them (creating dead/garbage records in the sealed file),
     * force a merge, verify the merge cleaned up correctly and all data is
     * still readable, then simulate a process restart and verify every key
     * is still readable - which requires buildKeyDirectory() to have
     * correctly recovered entries from the merge's hint file.
     */
    @Test
    void addManyKeysForceMergeAndRecoverAfterRestart() throws Exception {
        int keyCount = 30;
        Map<String, byte[]> expected = new HashMap<>();

        // 1. Insert enough unique keys that the active file rolls over at least once.
        for (int i = 0; i < keyCount; i++) {
            String key = String.format("key-%04d", i);
            byte[] value = randomValue(VALUE_SIZE, i);
            bitcask.put(key.getBytes(StandardCharsets.UTF_8), value);
            expected.put(key, value);
        }

        long dataFilesBeforeMerge = countFilesEndingWith(".data");
        assertTrue(dataFilesBeforeMerge >= 2,
                "Expected at least one sealed file plus the active file before merging, found " + dataFilesBeforeMerge);

        // 2. Overwrite a subset of the earlier keys. Their old records (in the
        //    now-sealed file) become dead/garbage that merge() must discard,
        //    since keyDirectory now points those keys at the active file.
        for (int i = 0; i < 10; i++) {
            String key = String.format("key-%04d", i);
            byte[] newValue = randomValue(VALUE_SIZE, 1000 + i);
            bitcask.put(key.getBytes(StandardCharsets.UTF_8), newValue);
            expected.put(key, newValue);
        }

        assertAllValuesMatch(bitcask, expected, "before merge");

        // 3. Force a merge synchronously (bypassing the async size/garbage-triggered path).
        bitcask.merge();

        // 4. Verify merge cleaned up on disk: no leftover .tmp merge-in-progress
        //    files, no leftover .odata retired files, and at least one .hint
        //    file was produced for the compacted data.
        assertEquals(0, countFilesEndingWith(".tmp"), "No .tmp merge files should remain after merge");
        assertEquals(0, countFilesEndingWith(".odata"), "Retired data files should be deleted after merge");
        assertTrue(countFilesEndingWith(".hint") >= 1, "Merge should have produced at least one hint file");

        // 5. All keys (original + overwritten) must still resolve correctly post-merge.
        assertAllValuesMatch(bitcask, expected, "after merge");

        // 6. Simulate a restart: close and reopen against the same directory.
        //    The constructor rebuilds keyDirectory via buildKeyDirectory(),
        //    which per the new logic should use the hint file for the merged
        //    sequence instead of re-scanning its data file.
        bitcask.close();
        bitcask = null;

        Bitcask reopened = new Bitcask();
        try {
            assertAllValuesMatch(reopened, expected, "after restart (hint-file recovery)");
        } finally {
            reopened.close();
        }
    }

    @Test
    void buildKeyDirectoryRecoversFromHintFileEvenIfDataFileIsGone() throws Exception {
        int keyCount = 25;
        Map<String, byte[]> expected = new HashMap<>();

        for (int i = 0; i < keyCount; i++) {
            String key = "hk-" + i;
            byte[] value = randomValue(VALUE_SIZE, i);
            bitcask.put(key.getBytes(StandardCharsets.UTF_8), value);
            expected.put(key, value);
        }

        bitcask.merge();

        Path hintFile = findSingleFileEndingWith(".hint");
        String sequence = hintFile.getFileName().toString().substring(
                0, hintFile.getFileName().toString().indexOf('.'));
        Path mergedDataFile = DATA_DIR.resolve(sequence + ".data");

        assertTrue(Files.exists(mergedDataFile), "Merged data file should exist right after merge");

        // Remove the merged data file but keep its hint file.
        Files.delete(mergedDataFile);

        bitcask.close();
        bitcask = null;

        // Constructor must NOT throw: buildKeyDirectory() should recover
        // every merged key's entry purely from the .hint file.
        Bitcask reopened = assertDoesNotThrow(Bitcask::new,
                "buildKeyDirectory() should succeed using only the hint file when the merged data file is missing");

        try {
            // Reading a merged key's value should now fail (bytes are gone),
            // proving the index entry existed (built from the hint file) but
            // the underlying data file did not.
            byte[] anyMergedKey = ("hk-" + 0).getBytes(StandardCharsets.UTF_8);
            assertThrows(IOException.class, () -> reopened.get(anyMergedKey),
                    "Expected a read failure since the merged data file was deleted, "
                            + "even though the key directory entry (from the hint file) exists");
        } finally {
            reopened.close();
        }
    }

    // ---- helpers ----

    private void assertAllValuesMatch(Bitcask target, Map<String, byte[]> expected, String phase) throws IOException {
        for (Map.Entry<String, byte[]> e : expected.entrySet()) {
            byte[] actual = target.get(e.getKey().getBytes(StandardCharsets.UTF_8));
            assertNotNull(actual, "Missing value for key " + e.getKey() + " (" + phase + ")");
            assertArrayEquals(e.getValue(), actual, "Mismatched value for key " + e.getKey() + " (" + phase + ")");
        }
    }

    private long countFilesEndingWith(String suffix) throws IOException {
        try (Stream<Path> walk = Files.walk(DATA_DIR)) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(suffix))
                    .count();
        }
    }

    private Path findSingleFileEndingWith(String suffix) throws IOException {
        List<Path> matches;
        try (Stream<Path> walk = Files.walk(DATA_DIR)) {
            matches = walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(suffix))
                    .toList();
        }
        assertEquals(1, matches.size(),
                "Expected exactly one file ending with " + suffix + ", found " + matches.size() + ": " + matches);
        return matches.get(0);
    }

    private byte[] randomValue(int size, long seed) {
        byte[] value = new byte[size];
        new Random(seed).nextBytes(value);
        return value;
    }

    private void cleanDataDir() throws IOException {
        if (Files.exists(DATA_DIR)) {
            try (Stream<Path> walk = Files.walk(DATA_DIR)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }
        Files.createDirectories(DATA_DIR);
    }
}