package com.webdev;

import com.webdev.config.KafkaConfig;
import com.webdev.server.BitcaskServer;
import com.webdev.service.BitcaskConsumerService;
import com.webdev.service.KafkaStreamsRainDetectionService;
import com.webdev.service.ParquetArchiverService;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.IOException;
import java.io.UncheckedIOException;

public class Main {
    public static void main(String[] args) throws IOException {

        Dotenv dotenv = Dotenv.load();
        KafkaConfig config = new KafkaConfig(dotenv.get("KAFKA_BOOTSTRAP_SERVER"), dotenv.get("SCHEMA_REGISTRY_URL"));
        System.setProperty(
                "org.apache.avro.SERIALIZABLE_PACKAGES",
                "gen"
        );

        KafkaStreamsRainDetectionService rainDetectionService = new KafkaStreamsRainDetectionService(config);
        rainDetectionService.process();

        Bitcask bitcask = new Bitcask(dotenv.get("BITCASK_DATA_DIR"));
        BitcaskConsumerService bitcaskConsumerService = new BitcaskConsumerService(bitcask, config);
        bitcaskConsumerService.start();


        java.nio.file.Path parquetDir = java.nio.file.Path.of(dotenv.get("PARQUET_OUTPUT_DIR"));
        ParquetArchiverService parquetService = new ParquetArchiverService(parquetDir, config);
        parquetService.start();

        BitcaskServer server = new BitcaskServer(9090, 2, bitcask);
        Thread serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, "bitcask-server");
        serverThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                bitcaskConsumerService.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            server.stop();
            rainDetectionService.stop();
            parquetService.stop();
        }, "shutdown-hook"));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                bitcaskConsumerService.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            rainDetectionService.stop();
            parquetService.stop();
        }, "shutdown-hook"));
    }
}