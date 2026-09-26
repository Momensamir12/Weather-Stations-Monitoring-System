package com.webdev;

import com.webdev.config.AppBootstrap;
import com.webdev.config.KafkaConfig;
import com.webdev.server.BitcaskServer;
import com.webdev.service.BitcaskConsumerService;
import com.webdev.service.KafkaStreamsRainDetectionService;
import com.webdev.service.ManagedService;
import com.webdev.service.ParquetArchiverService;
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final int BITCASK_SERVER_THREAD_POOL_SIZE = 2;
    private static final long SERVER_STOP_JOIN_TIMEOUT_MILLIS = 10_000;

    public static void main(String[] args) throws IOException {

        Dotenv dotenv = AppBootstrap.loadEnv();

        KafkaConfig config = new KafkaConfig(
                dotenv.get("KAFKA_BOOTSTRAP_SERVER"),
                dotenv.get("SCHEMA_REGISTRY_URL"));

        Bitcask bitcask = new Bitcask(dotenv.get("BITCASK_DATA_DIR"));

        int bitcaskServerPort = Integer.parseInt(dotenv.get("BITCASK_SERVER_PORT", "9090"));

        KafkaStreamsRainDetectionService rainDetectionService = new KafkaStreamsRainDetectionService(config);
        BitcaskConsumerService bitcaskConsumerService = new BitcaskConsumerService(bitcask, config);
        ParquetArchiverService parquetService = new ParquetArchiverService(
                Path.of(dotenv.get("PARQUET_OUTPUT_DIR")), config);
        BitcaskServer bitcaskServer = new BitcaskServer(bitcaskServerPort, BITCASK_SERVER_THREAD_POOL_SIZE, bitcask);

        List<ManagedService> services = new ArrayList<>();
        services.add(rainDetectionService);
        services.add(bitcaskConsumerService);
        services.add(parquetService);
        services.add(asManagedService(bitcaskServer));

        services.forEach(ManagedService::start);
        log.info("All services started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping services");

            List<ManagedService> reversed = new ArrayList<>(services);
            Collections.reverse(reversed);
            reversed.forEach(service -> {
                try {
                    service.stop();
                } catch (Exception e) {
                    log.error("Error stopping {}", service.getClass().getSimpleName(), e);
                }
            });

            try {
                bitcask.close();
                log.info("Bitcask closed");
            } catch (Exception e) {
                log.error("Error closing Bitcask", e);
            }

            log.info("Shutdown complete");
        }, "shutdown-hook"));
    }

    private static ManagedService asManagedService(BitcaskServer server) {
        return new ManagedService() {
            private Thread serverThread;

            @Override
            public void start() {
                serverThread = new Thread(() -> {
                    try {
                        server.start();
                    } catch (IOException e) {
                        log.error("Bitcask server failed", e);
                    }
                }, "bitcask-server");
                serverThread.start();
                log.info("Bitcask server started");
            }

            @Override
            public void stop() {
                server.stop();
                try {
                    serverThread.join(SERVER_STOP_JOIN_TIMEOUT_MILLIS);
                    if (serverThread.isAlive()) {
                        log.warn("Bitcask server thread did not stop within {}ms", SERVER_STOP_JOIN_TIMEOUT_MILLIS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
    }
}