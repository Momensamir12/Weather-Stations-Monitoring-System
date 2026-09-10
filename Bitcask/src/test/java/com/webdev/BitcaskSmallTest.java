package com.webdev;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

public class BitcaskSmallTest {

    private static final String DIR = "Bitcask/bitcask-files/";

    public static void main(String[] args) throws Exception {
        cleanDirectory();

        // Phase 1: write, forcing rotation across a few files.
        Bitcask bitcask = new Bitcask();

        for (int i = 0; i < 15; i++) {
            put(bitcask, "filler-" + i, "filler-value-" + i);
        }

        // key A: write once, bury it under filler to force a rotation, then overwrite.
        // The LAST value ("A-v2") is the one that must survive.
        put(bitcask, "A", "A-v1");
        for (int i = 15; i < 30; i++) {
            put(bitcask, "filler-" + i, "filler-value-" + i);
        }
        put(bitcask, "A", "A-v2");

        for (int i = 30; i < 45; i++) {
            put(bitcask, "filler-" + i, "filler-value-" + i);
        }
        put(bitcask, "B", "B-only");

        bitcask.close();

        System.out.println("---- files on disk after phase 1 ----");
        listFiles();

        // Phase 2: brand new instance -> forces buildKeyDirectory() to scan from disk.
        System.out.println("---- reopening (this will print the rebuilt keyDir) ----");
        Bitcask reopened = new Bitcask();

        System.out.println("---- verifying ----");
        check(reopened, "A", "A-v2");
        check(reopened, "B", "B-only");

        reopened.close();
    }

    private static void put(Bitcask b, String k, String v) throws Exception {
        b.put(k.getBytes(StandardCharsets.UTF_8), v.getBytes(StandardCharsets.UTF_8));
    }

    private static void check(Bitcask b, String key, String expected) throws Exception {
        byte[] raw = b.get(key.getBytes(StandardCharsets.UTF_8));
        String actual = raw == null ? null : new String(raw, StandardCharsets.UTF_8);
        String verdict = expected.equals(actual) ? "PASS" : "FAIL";
        System.out.printf("%s key=%s expected=%s actual=%s%n", verdict, key, expected, actual);
    }

    private static void cleanDirectory() throws IOException {
        Path dir = Path.of(DIR);
        Files.createDirectories(dir);
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(p -> p.toFile().delete());
        }
    }

    private static void listFiles() throws IOException {
        try (Stream<Path> stream = Files.list(Path.of(DIR))) {
            stream.sorted(Comparator.comparingInt(p -> {
                        String n = p.getFileName().toString();
                        int dot = n.lastIndexOf('.');
                        return Integer.parseInt(n.substring(0, dot));
                    }))
                    .forEach(p -> System.out.println(p.getFileName()));
        }
    }
}