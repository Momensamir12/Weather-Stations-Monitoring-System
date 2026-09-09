package com.webdev;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Standalone loop-based test harness for Bitcask.
 * Not JUnit on purpose, to match the quick "run main and read stdout" style
 * you already had — swap to JUnit later if you want it wired into a build.
 */
public class BitcaskLoopTest {

    public static void main(String[] args) throws Exception {
        Bitcask bitcask = new Bitcask(); // adjust if your constructor takes a directory path

        int totalFailures = 0;
        totalFailures += testBasicPutGet(bitcask);
        totalFailures += testOverwrite(bitcask);
        totalFailures += testManyKeysLoop(bitcask, 1000);
        totalFailures += testRepeatedUpdatesSingleKey(bitcask, 500);
        totalFailures += testMissingKey(bitcask);

        System.out.println();
        if (totalFailures == 0) {
            System.out.println("ALL TESTS PASSED");
        } else {
            System.out.println(totalFailures + " ASSERTION(S) FAILED");
        }
    }

    // ---- individual test cases ----

    /** Distinct keys, single write each, verify every one reads back correctly. */
    private static int testBasicPutGet(Bitcask bitcask) throws Exception {
        System.out.println("== testBasicPutGet ==");
        int failures = 0;
        Map<String, String> expected = new HashMap<>();

        for (int i = 0; i < 20; i++) {
            String key = "station-" + i;
            String value = "humidity=" + (50 + i);
            put(bitcask, key, value);
            expected.put(key, value);
        }
        for (Map.Entry<String, String> e : expected.entrySet()) {
            failures += assertEquals(e.getValue(), get(bitcask, e.getKey()), "basic key " + e.getKey());
        }
        return failures;
    }

    /** Same key written multiple times — only the latest value should ever come back. */
    private static int testOverwrite(Bitcask bitcask) throws Exception {
        System.out.println("== testOverwrite ==");
        int failures = 0;
        String key = "station-overwrite";

        put(bitcask, key, "v1");
        put(bitcask, key, "v2");
        put(bitcask, key, "v3-final");

        failures += assertEquals("v3-final", get(bitcask, key), "overwrite should return latest value");
        return failures;
    }

    /**
     * The real stress test: write N distinct keys with random values, then read them
     * back in a different (shuffled) order against an in-memory reference map. Reading
     * out of insertion order helps catch bugs where offset tracking only happens to
     * work when reads follow writes sequentially.
     */
    private static int testManyKeysLoop(Bitcask bitcask, int count) throws Exception {
        System.out.println("== testManyKeysLoop (" + count + " keys) ==");
        int failures = 0;
        Random random = new Random(42); // fixed seed -> reproducible failures
        Map<String, String> reference = new HashMap<>();

        for (int i = 0; i < count; i++) {
            String key = "key-" + i;
            String value = randomValue(random, 16);
            put(bitcask, key, value);
            reference.put(key, value);
        }

        List<String> keys = new ArrayList<>(reference.keySet());
        Collections.shuffle(keys, random);

        for (String key : keys) {
            failures += assertEquals(reference.get(key), get(bitcask, key), "loop key " + key);
        }
        System.out.println(count + " keys verified, " + failures + " mismatches");
        return failures;
    }

    /** Hammer one key with many sequential updates — checks your offset/keydir update path under churn. */
    private static int testRepeatedUpdatesSingleKey(Bitcask bitcask, int updates) throws Exception {
        System.out.println("== testRepeatedUpdatesSingleKey (" + updates + " updates) ==");
        String key = "hot-key";
        String lastValue = null;

        for (int i = 0; i < updates; i++) {
            lastValue = "update-" + i;
            put(bitcask, key, lastValue);
        }
        return assertEquals(lastValue, get(bitcask, key), "hot key should reflect last of " + updates + " updates");
    }

    /** A key that was never written should come back as null (adjust if your API throws instead). */
    private static int testMissingKey(Bitcask bitcask) throws Exception {
        System.out.println("== testMissingKey ==");
        String result = get(bitcask, "this-key-was-never-written");
        return assertEquals(null, result, "missing key should return null");
    }

    // ---- helpers ----

    private static void put(Bitcask bitcask, String key, String value) throws Exception {
        bitcask.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    private static String get(Bitcask bitcask, String key) throws Exception {
        byte[] raw = bitcask.get(key.getBytes(StandardCharsets.UTF_8));
        return raw == null ? null : new String(raw, StandardCharsets.UTF_8);
    }

    private static String randomValue(Random random, int len) {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private static int assertEquals(String expected, String actual, String label) {
        if (Objects.equals(expected, actual)) {
            return 0;
        }
        System.out.println("FAIL [" + label + "]: expected=\"" + expected + "\" actual=\"" + actual + "\"");
        return 1;
    }
}