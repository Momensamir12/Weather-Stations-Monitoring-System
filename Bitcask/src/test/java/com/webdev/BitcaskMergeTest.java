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
 * Deterministic correctness test for Bitcask.merge().
 *
 * The previous version of this test tried to coax an automatic, background-triggered
 * merge into firing by writing enough churn to cross the internal size/dead-byte
 * thresholds, then scraped stdout for a "MERGE TRIGGERED" line to know whether it had
 * run. That turned out to be fragile in two independent ways: it silently reports
 * "0 merges" if that println is ever removed or reworded, and it races close()
 * against a still-running background merge unless close() explicitly blocks on the
 * merge future (which it currently doesn't).
 *
 * merge() is public, so this version calls it directly, synchronously, on the
 * test's own thread instead. That removes both problems: there's no background
 * thread to race against, and success/failure is whatever merge() itself does
 * (throws, or doesn't) - not a log line, not a timing guess.
 *
 * The test is built in three phases so the expected on-disk state after merge is
 * exactly computable, not just "smaller than before":
 *
 *   1. Write many unique keys (zero overwrites -> zero garbage), ending with one
 *      deliberately oversized put guaranteed to push nextWriteOffset past
 *      sizeThreshold by itself. That forces a rotation right there, so whatever
 *      file is "active" going into phase 2 is guaranteed to start empty - making
 *      phase 2's byte budget exact instead of dependent on leftover space in
 *      whatever file happened to be active at the time.
 *   2. Overwrite a known subset of those keys with fresh values, staying safely
 *      under sizeThreshold so this phase causes zero further rotations - meaning
 *      the dead bytes it creates just sit there untouched until we explicitly
 *      merge, and the fresh values never land in a file merge() will touch (an
 *      active file is always excluded from merge's input).
 *   3. Call merge() directly, then assert EXACTLY: every live key's latest value
 *      survives a full disk-scan reopen, the bytes actually on disk equal the sum
 *      of ONLY the current values (proving dead records were dropped, not just
 *      copied forward), and no .tmp/.odata files remain.
 */
public class BitcaskMergeTest {

    private static final String DIR = "Bitcask/bitcask-files/";

    private static final int UNIQUE_KEY_COUNT = 200;
    private static final int VALUE_SIZE = 15_000;
    // Must be >= Bitcask's actual sizeThreshold for the forced-rotation trick below
    // to work. If you change sizeThreshold, bump this to match.
    private static final long SIZE_THRESHOLD_ASSUMPTION = 1024 * 1024;
    private static final int OVERWRITE_COUNT = 40; // must be < UNIQUE_KEY_COUNT

    public static void main(String[] args) throws Exception {
        cleanDirectory();
        Random random = new Random(7);

        Map<KeyWrapper, byte[]> expectedFinalValues = new HashMap<>();
        long totalBytesWritten = 0;

        Bitcask bitcask = new Bitcask();

        // ---- phase 1: unique keys, zero garbage ----
        System.out.println("== phase 1: " + UNIQUE_KEY_COUNT + " unique keys, no overwrites ==");
        byte[][] phase1Keys = new byte[UNIQUE_KEY_COUNT][];
        for (int i = 0; i < UNIQUE_KEY_COUNT; i++) {
            byte[] key = ("p1-key-" + i).getBytes();
            byte[] value = randomBytes(random, VALUE_SIZE);
            phase1Keys[i] = key;
            bitcask.put(key, value);
            expectedFinalValues.put(new KeyWrapper(key), value);
            totalBytesWritten += 8 + key.length + value.length;
        }

        // ---- forced clean rotation boundary before phase 2 ----
        System.out.println("== forcing a clean rotation boundary before phase 2 ==");
        byte[] flushKey = "flush-boundary-key".getBytes();
        byte[] flushValue = randomBytes(random, (int) SIZE_THRESHOLD_ASSUMPTION + 1024);
        bitcask.put(flushKey, flushValue);
        expectedFinalValues.put(new KeyWrapper(flushKey), flushValue);
        totalBytesWritten += 8 + flushKey.length + flushValue.length;
        // This single record is bigger than sizeThreshold on its own, so it guarantees
        // a rotation happens right after this put - regardless of how much leftover
        // space was already used in whatever file was active. Going into phase 2, the
        // active file is guaranteed fresh.

        // ---- phase 2: overwrite a known subset, staying under the rotation budget ----
        System.out.println("== phase 2: overwriting " + OVERWRITE_COUNT + " of those keys (creates reclaimable garbage) ==");
        for (int i = 0; i < OVERWRITE_COUNT; i++) {
            byte[] key = phase1Keys[i];
            byte[] value = randomBytes(random, VALUE_SIZE);
            bitcask.put(key, value);
            expectedFinalValues.put(new KeyWrapper(key), value); // overwrites the phase-1 value in our reference map too
            totalBytesWritten += 8 + key.length + value.length;
        }
        // OVERWRITE_COUNT * (8 + keyLen + VALUE_SIZE) is comfortably under sizeThreshold,
        // so no rotation happens here - and since mergeCompactionPolicy() is only ever
        // checked inside a rotation, no auto-triggered merge can fire during this phase.

        // ---- phase 3: call merge() directly ----
        System.out.println("== calling merge() directly ==");
        boolean mergeThrew = false;
        try {
            bitcask.merge();
        } catch (Exception e) {
            mergeThrew = true;
            System.out.println("FAIL: merge() threw an exception:");
            e.printStackTrace();
        }
        System.out.println(mergeThrew ? "FAIL: merge() did not complete cleanly" : "OK: merge() completed without throwing");

        bitcask.close();

        // ---- verification ----
        System.out.println("---- checking for leftover intermediate files ----");
        List<String> strayFiles = new ArrayList<>(listFilesEndingWith(".tmp"));
        strayFiles.addAll(listFilesEndingWith(".odata"));
        boolean noStrayFiles = strayFiles.isEmpty();
        System.out.println(noStrayFiles ? "OK: no .tmp or .odata files left on disk"
                : "FAIL: found " + strayFiles.size() + " leftover files: " + strayFiles);

        long expectedLiveFloor = 0;
        for (Map.Entry<KeyWrapper, byte[]> e : expectedFinalValues.entrySet()) {
            expectedLiveFloor += 8 + e.getKey().bytes.length + e.getValue().length;
        }
        long liveDiskBytes = sumDataFileSizes();

        System.out.println("---- space reclamation (exact check) ----");
        System.out.println("total bytes ever written (including overwritten garbage): " + totalBytesWritten);
        System.out.println("expected live floor (sum of only the CURRENT value per key): " + expectedLiveFloor);
        System.out.println("bytes actually on disk across .data files: " + liveDiskBytes);
        boolean exactSpaceReclaimed = liveDiskBytes == expectedLiveFloor;
        System.out.println(exactSpaceReclaimed
                ? "OK: on-disk bytes exactly match the live-data floor -> dead records were dropped, nothing extra"
                : "FAIL: on-disk bytes don't match the live-data floor (diff=" + (liveDiskBytes - expectedLiveFloor) + ")");

        System.out.println("---- reopening fresh instance (forces buildKeyDirectory() over post-merge files) ----");
        Bitcask reader = new Bitcask();

        int failures = 0;
        List<KeyWrapper> keyList = new ArrayList<>(expectedFinalValues.keySet());
        Collections.shuffle(keyList, random);
        for (KeyWrapper kw : keyList) {
            byte[] expected = expectedFinalValues.get(kw);
            byte[] actual = reader.get(kw.bytes);
            if (!Arrays.equals(expected, actual)) {
                failures++;
                System.out.printf("FAIL key=%s expectedLen=%d actualLen=%d%n",
                        new String(kw.bytes), expected.length, actual == null ? -1 : actual.length);
            }
        }
        reader.close();

        System.out.println();
        System.out.println(expectedFinalValues.size() + " keys verified, " + failures + " mismatches");

        boolean allPassed = !mergeThrew && noStrayFiles && exactSpaceReclaimed && failures == 0;
        System.out.println(allPassed ? "ALL MERGE TESTS PASSED" : "SOME MERGE TESTS FAILED");
    }

    private static byte[] randomBytes(Random random, int len) {
        byte[] bytes = new byte[len];
        random.nextBytes(bytes);
        return bytes;
    }

    private static void cleanDirectory() throws IOException {
        Path dir = Path.of(DIR);
        Files.createDirectories(dir);
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(p -> p.toFile().delete());
        }
    }

    private static List<String> listFilesEndingWith(String suffix) throws IOException {
        try (Stream<Path> stream = Files.list(Path.of(DIR))) {
            return stream.map(p -> p.getFileName().toString())
                    .filter(name -> name.endsWith(suffix))
                    .toList();
        }
    }

    private static long sumDataFileSizes() throws IOException {
        try (Stream<Path> stream = Files.list(Path.of(DIR))) {
            return stream.filter(p -> p.getFileName().toString().endsWith(".data"))
                    .mapToLong(p -> p.toFile().length())
                    .sum();
        }
    }

    /** byte[] has no natural equals/hashCode, so wrap it for use as a HashMap key. */
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