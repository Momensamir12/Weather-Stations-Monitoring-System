package com.webdev.service;

import com.webdev.avro.AvroWeatherStatusMessage;
import com.webdev.config.KafkaConfig;
import com.webdev.constants.Topics;
import com.webdev.record.PartitionKey;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParquetArchiverService implements ManagedService {

    private static final Logger log = LoggerFactory.getLogger(ParquetArchiverService.class);

    private static final int BATCH_SIZE = 10_000;
    private static final long FLUSH_INTERVAL_MILLIS = 30_000;

    private final java.nio.file.Path outputDir;
    private final KafkaConfig kafkaConfig;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile KafkaConsumer<String, AvroWeatherStatusMessage> consumer;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "parquet archiver"));

    private Future<?> task;

    private final List<ConsumerRecord<String, AvroWeatherStatusMessage>> buffer = new ArrayList<>();
    private long lastFlushMillis = System.currentTimeMillis();

    public ParquetArchiverService(java.nio.file.Path outputDir, KafkaConfig kafkaConfig) {
        this.outputDir = outputDir;
        this.kafkaConfig = kafkaConfig;
    }

    public void start() {
        running.set(true);
        task = executor.submit(this::runLoop);
        log.info("Started parquet archiver thread, outputDir={}", outputDir);
    }

    private void runLoop() {
        try (var c = new KafkaConsumer<String, AvroWeatherStatusMessage>(
                kafkaConfig.consumerConfig("parquet-archiver"))) {

            this.consumer = c;
            c.subscribe(List.of(Topics.STATIONS_STATUS));

            while (running.get()) {
                var records = c.poll(java.time.Duration.ofMillis(500));
                records.forEach(buffer::add);

                boolean batchFull = buffer.size() >= BATCH_SIZE;
                boolean timeElapsed =
                        System.currentTimeMillis() - lastFlushMillis >= FLUSH_INTERVAL_MILLIS;

                if (!buffer.isEmpty() && (batchFull || timeElapsed)) {
                    flush();
                    c.commitSync();          // commit only after the files are on disk
                    lastFlushMillis = System.currentTimeMillis();
                }
            }

            // drain whatever's left before the consumer closes
            if (!buffer.isEmpty()) {
                log.info("Draining {} remaining buffered records before shutdown", buffer.size());
                flush();
                c.commitSync();
            }

        } catch (WakeupException e) {
            log.info("parquet-archiver received shutdown signal, exiting run loop");
        } catch (IOException e) {
            log.error("parquet-archiver failed while flushing to disk, thread terminating", e);
            throw new UncheckedIOException(e);
        }
    }

    private void flush() throws IOException {
        long start = System.currentTimeMillis();
        int recordCount = buffer.size();

        Map<PartitionKey, List<AvroWeatherStatusMessage>> byPartition = new HashMap<>();

        for (var r : buffer) {
            AvroWeatherStatusMessage msg = r.value();
            LocalDate date = Instant.ofEpochSecond(msg.getStatusTimeStamp())
                    .atZone(ZoneOffset.UTC)
                    .toLocalDate();
            byPartition
                    .computeIfAbsent(new PartitionKey(msg.getStationId(), date), k -> new ArrayList<>())
                    .add(msg);
        }

        for (var entry : byPartition.entrySet()) {
            try {
                writePartitionFile(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.error("Failed writing partition file for stationId={} date={}, {} records lost from this flush",
                        entry.getKey().stationId(), entry.getKey().date(), entry.getValue().size(), e);
                throw e;
            }
        }

        buffer.clear();

        log.info("Flushed {} records across {} partitions in {} ms",
                recordCount, byPartition.size(), System.currentTimeMillis() - start);
    }

    private void writePartitionFile(PartitionKey key, List<AvroWeatherStatusMessage> messages)
            throws IOException {

        java.nio.file.Path partitionDir = outputDir
                .resolve("station_id=" + key.stationId())
                .resolve("date=" + key.date());
        Files.createDirectories(partitionDir);

        String baseName = "part-" + System.currentTimeMillis() + "-" + UUID.randomUUID();
        java.nio.file.Path tempPath = partitionDir.resolve(baseName + ".parquet.tmp");
        java.nio.file.Path finalPath = partitionDir.resolve(baseName + ".parquet");

        Path hadoopTempPath = new Path(tempPath.toString());

        try (ParquetWriter<AvroWeatherStatusMessage> writer = AvroParquetWriter
                .<AvroWeatherStatusMessage>builder(hadoopTempPath)
                .withSchema(AvroWeatherStatusMessage.getClassSchema())
                .withDataModel(SpecificData.get())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (AvroWeatherStatusMessage msg : messages) {
                writer.write(msg);
            }
        }

        Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);

        log.debug("Wrote {} records to {}", messages.size(), finalPath);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        log.info("Stopping parquet archiver thread");
        var c = consumer;
        if (c != null) c.wakeup();
        try {
            task.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Exception while stopping parquet archiver task", e);
            throw new RuntimeException(e);
        }
        finally {
            executor.shutdownNow();
        }
    }

}