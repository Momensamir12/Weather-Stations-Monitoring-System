package com.webdev;

import java.nio.file.*;
import java.util.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BitcaskMergeOrderTest {
    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("bitcask-order-");
        Map<String, byte[]> expected = new HashMap<>();

        try (Bitcask b = new Bitcask(dir.toString())) {
            // Phase 1: write until the first rollover (2.data appears), then stop immediately,
            // so every key is still live in 1.data when the merge reads it
            int i = 0;
            while (!Files.exists(dir.resolve("2.data"))) {
                String k = "station-" + (i % 1000 + 1);
                byte[] v = ("old" + i + "-" + "x".repeat(190)).getBytes(UTF_8);
                b.put(k.getBytes(UTF_8), v);
                expected.put(k, v);
                i++;
            }
            Thread.sleep(1500);   // merge finishes: expect 3.data + 3.hint

            // Phase 2: overwrite every key into the active file (2.data), no further rollover
            for (int j = 1; j <= 1000; j++) {
                String k = "station-" + j;
                byte[] v = ("new" + j + "-" + "y".repeat(190)).getBytes(UTF_8);
                b.put(k.getBytes(UTF_8), v);
                expected.put(k, v);
            }
        }

        try (var s = Files.list(dir)) {
            s.sorted().forEach(p -> System.out.println(p.getFileName() + "  " + p.toFile().length()));
        }

        try (Bitcask b = new Bitcask(dir.toString())) {
            int stale = 0, missing = 0;
            for (var e : expected.entrySet()) {
                byte[] got = b.get(e.getKey().getBytes(UTF_8));
                if (got == null) missing++;
                else if (!Arrays.equals(got, e.getValue())) stale++;
            }
            System.out.println("missing=" + missing + " stale=" + stale + " of " + expected.size());
        }
    }
}