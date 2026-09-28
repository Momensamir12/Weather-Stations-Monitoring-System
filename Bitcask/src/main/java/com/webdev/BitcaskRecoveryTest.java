package com.webdev;

import java.nio.file.*;
import java.util.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BitcaskRecoveryTest {
    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("bitcask-recovery-");
        Map<String, byte[]> expected = new HashMap<>();

        try (Bitcask b = new Bitcask(dir.toString())) {
            for (int i = 0; i < 500_000; i++) {
                String k = "station-" + (i % 1000 + 1);
                byte[] v = ("v" + i + "-" + "x".repeat(190)).getBytes(UTF_8);
                b.put(k.getBytes(UTF_8), v);
                expected.put(k, v);          // last write wins
            }
            Thread.sleep(2000);              // let any in-flight merge finish
        }

        long t0 = System.nanoTime();
        try (Bitcask b = new Bitcask(dir.toString())) {
            System.out.printf("startup: %.1f ms%n", (System.nanoTime() - t0) / 1e6);
            int stale = 0, missing = 0;
            for (var e : expected.entrySet()) {
                byte[] got = b.get(e.getKey().getBytes(UTF_8));
                if (got == null) missing++;
                else if (!Arrays.equals(got, e.getValue())) stale++;
            }
            System.out.println("missing=" + missing + " stale=" + stale + " of " + expected.size());
        }
        System.out.println("dir: " + dir);
    }
}