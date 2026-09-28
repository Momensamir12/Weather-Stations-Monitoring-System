package com.webdev;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Standalone throughput/latency benchmark for Bitcask, with Kafka out of the picture.
 * Single-threaded on purpose — it mirrors BitcaskConsumerService's single writer thread,
 * so the numbers here are directly comparable to the "avg put ms" / throughput columns
 * you were seeing in the metrics.bitcask log.
 *
 * Run directly, e.g.:
 *   java com.webdev.BitcaskBenchmark --records=200000 --keys=1000 --valueSize=200
 *
 * Args (all optional, "--name=value"):
 *   records     put() calls to benchmark, after warmup                (default 200000)
 *   warmup      put() calls run before timing starts                  (default 5000)
 *   keys        distinct keys reused/cycled; 0 = every put is a
 *               brand-new key, never overwritten                      (default 1000)
 *   valueSize   value payload size in bytes                           (default 200)
 *   dir         base directory for the store; omit for a fresh temp
 *               dir (printed at startup). Point this at your real
 *               production volume/mount to benchmark the real I/O
 *               path rather than your host's local temp disk.
 *   readSample  get() calls sampled after the write phase              (default 5000)
 *   keepDir     "false" deletes the store dir when done                (default true)
 *
 * Two runs worth comparing directly:
 *   --keys=1000   mirrors your BurstPublisher's station_id % 1000 pattern — every
 *                 key after the first 1000 writes is an overwrite, which accumulates
 *                 dead bytes and (given GARBAGE_THRESHOLD_BYTES=64KB) triggers merge()
 *                 on essentially every file rollover once warmed up.
 *   --keys=0      every key is unique, so there's never a dead byte to compact and
 *                 merge only ever fires off the 10-file-count threshold. If this run's
 *                 latency is dramatically smoother than --keys=1000, compaction
 *                 contention — not raw disk I/O — is your main source of variance.
 *
 * To catch GC pauses / I/O stalls the way your project's deliverable (G) asks for, wrap
 * this with JFR, e.g.:
 *   java -XX:StartFlightRecording=duration=120s,filename=bitcask-bench.jfr \
 *        com.webdev.BitcaskBenchmark --records=500000
 */
public class BitcaskBenchmark {

    public static void main(String[] args) throws Exception {
        Options opts = Options.parse(args);

        Path dir = opts.dir != null ? Path.of(opts.dir)
                : Files.createTempDirectory("bitcask-bench-");

        System.out.println("Bitcask benchmark");
        System.out.println("  store dir      : " + dir.toAbsolutePath());
        System.out.println("  records        : " + opts.records);
        System.out.println("  warmup         : " + opts.warmup);
        System.out.println("  key cardinality: " + (opts.keys == 0 ? "unique (no reuse)" : opts.keys));
        System.out.println("  value size     : " + opts.valueSize + " bytes");
        System.out.println();

        Bitcask bitcask = new Bitcask(dir.toString());

        try {
            runPutBenchmark(bitcask, opts);
            runGetBenchmark(bitcask, opts);
        } finally {
            bitcask.close();
        }

        // give any in-flight background merge a moment to settle before reporting layout
        Thread.sleep(2000);
        reportDirectoryLayout(dir);

        if (!opts.keepDir) {
            deleteRecursively(dir);
            System.out.println("\n(store directory deleted)");
        }
    }

    // ------------------------------------------------------------------
    // PUT benchmark
    // ------------------------------------------------------------------

    private static void runPutBenchmark(Bitcask bitcask, Options opts) throws IOException {
        Random rnd = ThreadLocalRandom.current();
        byte[] value = new byte[opts.valueSize];

        System.out.println("-- warmup (" + opts.warmup + " puts, not measured) --");
        for (int i = 0; i < opts.warmup; i++) {
            rnd.nextBytes(value);
            bitcask.put(keyFor(i, opts.keys), value);
        }

        System.out.println("-- measured put phase (" + opts.records + " puts) --");

        long[] latenciesNs = new long[opts.records];
        long windowStart = System.nanoTime();
        long runStart = windowStart;
        int windowCount = 0;
        long windowLatSum = 0;
        long windowLatMax = 0;

        for (int i = 0; i < opts.records; i++) {
            rnd.nextBytes(value);
            byte[] key = keyFor(opts.warmup + i, opts.keys);

            long t0 = System.nanoTime();
            bitcask.put(key, value);
            long dt = System.nanoTime() - t0;

            latenciesNs[i] = dt;
            windowCount++;
            windowLatSum += dt;
            if (dt > windowLatMax) windowLatMax = dt;

            long now = System.nanoTime();
            if (now - windowStart >= 1_000_000_000L) {
                double sec = (now - windowStart) / 1_000_000_000.0;
                System.out.printf("  window: %6d puts in %.3fs = %8.1f puts/sec, avg %.4fms, max %.4fms%n",
                        windowCount, sec, windowCount / sec,
                        (windowLatSum / (double) windowCount) / 1_000_000.0,
                        windowLatMax / 1_000_000.0);
                windowStart = now;
                windowCount = 0;
                windowLatSum = 0;
                windowLatMax = 0;
            }
        }

        long totalNs = System.nanoTime() - runStart;
        printStats("PUT", opts.records, totalNs, latenciesNs);
    }

    // ------------------------------------------------------------------
    // GET benchmark (sampled, after all writes are done)
    // ------------------------------------------------------------------

    private static void runGetBenchmark(Bitcask bitcask, Options opts) throws IOException {
        if (opts.readSample <= 0) return;

        int totalWritten = opts.warmup + opts.records;
        Random rnd = ThreadLocalRandom.current();

        System.out.println();
        System.out.println("-- get phase (" + opts.readSample + " sampled reads) --");

        long[] latenciesNs = new long[opts.readSample];
        long runStart = System.nanoTime();
        int misses = 0;

        for (int i = 0; i < opts.readSample; i++) {
            int idx = rnd.nextInt(totalWritten);
            byte[] key = keyFor(idx, opts.keys);

            long t0 = System.nanoTime();
            byte[] value = bitcask.get(key);
            long dt = System.nanoTime() - t0;

            latenciesNs[i] = dt;
            if (value == null) misses++;
        }

        long totalNs = System.nanoTime() - runStart;
        printStats("GET", opts.readSample, totalNs, latenciesNs);
        if (misses > 0) {
            System.out.println("  WARNING: " + misses + " keys returned null (unexpected — check correctness)");
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    /** cardinality == 0 means every index gets its own never-repeated key. */
    private static byte[] keyFor(int index, int cardinality) {
        int id = cardinality == 0 ? index : (index % cardinality) + 1;
        return ("station-" + id).getBytes(StandardCharsets.UTF_8);
    }

    private static void printStats(String label, int count, long totalNs, long[] latenciesNs) {
        long[] sorted = Arrays.copyOf(latenciesNs, latenciesNs.length);
        Arrays.sort(sorted);

        double totalSec = totalNs / 1_000_000_000.0;
        double avgMs = mean(sorted) / 1_000_000.0;
        double p50 = percentile(sorted, 50) / 1_000_000.0;
        double p95 = percentile(sorted, 95) / 1_000_000.0;
        double p99 = percentile(sorted, 99) / 1_000_000.0;
        double maxMs = sorted[sorted.length - 1] / 1_000_000.0;

        System.out.println();
        System.out.printf("== %s summary ==%n", label);
        System.out.printf("  count       : %d%n", count);
        System.out.printf("  wall time   : %.3fs%n", totalSec);
        System.out.printf("  throughput  : %.1f ops/sec%n", count / totalSec);
        System.out.printf("  avg latency : %.4fms%n", avgMs);
        System.out.printf("  p50 latency : %.4fms%n", p50);
        System.out.printf("  p95 latency : %.4fms%n", p95);
        System.out.printf("  p99 latency : %.4fms%n", p99);
        System.out.printf("  max latency : %.4fms%n", maxMs);
    }

    private static double mean(long[] sorted) {
        long sum = 0;
        for (long v : sorted) sum += v;
        return sum / (double) sorted.length;
    }

    private static double percentile(long[] sorted, double pct) {
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        idx = Math.max(0, Math.min(sorted.length - 1, idx));
        return sorted[idx];
    }

    private static void reportDirectoryLayout(Path dir) throws IOException {
        try (var stream = Files.list(dir)) {
            var files = stream.sorted().toList();
            long totalBytes = 0;
            int dataFiles = 0, hintFiles = 0, otherFiles = 0;

            for (Path p : files) {
                totalBytes += Files.size(p);
                String name = p.getFileName().toString();
                if (name.endsWith(".data")) dataFiles++;
                else if (name.endsWith(".hint")) hintFiles++;
                else otherFiles++;
            }

            System.out.println();
            System.out.println("== on-disk layout: " + dir.toAbsolutePath() + " ==");
            System.out.printf("  %d data files, %d hint files, %d other, %.2f MB total%n",
                    dataFiles, hintFiles, otherFiles, totalBytes / (1024.0 * 1024.0));
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (var stream = Files.walk(dir)) {
            stream.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException ignored) {
                }
            });
        }
    }

    // ------------------------------------------------------------------
    // CLI options
    // ------------------------------------------------------------------

    private static class Options {
        int records = 200_000;
        int warmup = 5_000;
        int keys = 1_000;
        int valueSize = 200;
        int readSample = 5_000;
        String dir = null;
        boolean keepDir = true;

        static Options parse(String[] args) {
            Options o = new Options();
            for (String arg : args) {
                if (!arg.startsWith("--") || !arg.contains("=")) continue;
                String[] kv = arg.substring(2).split("=", 2);
                switch (kv[0]) {
                    case "records" -> o.records = Integer.parseInt(kv[1]);
                    case "warmup" -> o.warmup = Integer.parseInt(kv[1]);
                    case "keys" -> o.keys = Integer.parseInt(kv[1]);
                    case "valueSize" -> o.valueSize = Integer.parseInt(kv[1]);
                    case "readSample" -> o.readSample = Integer.parseInt(kv[1]);
                    case "dir" -> o.dir = kv[1];
                    case "keepDir" -> o.keepDir = Boolean.parseBoolean(kv[1]);
                    default -> System.err.println("Unknown option: " + kv[0]);
                }
            }
            return o;
        }
    }
}