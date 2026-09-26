package com.webdev;

import com.webdev.config.AppBootstrap;
import com.webdev.config.KafkaConfig;
import com.webdev.infrastructure.KafkaStatusPublisher;
import com.webdev.infrastructure.StatusPublisher;
import com.webdev.service.ManagedService;
import com.webdev.service.OpenMeteoService;
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final int OPEN_METEO_STATION_ID = 100;
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(1);

    public static void main(String[] args) {

        Dotenv dotEnv = AppBootstrap.loadEnv();

        KafkaConfig config = new KafkaConfig(
                dotEnv.get("KAFKA_BOOTSTRAP_SERVER"),
                dotEnv.get("SCHEMA_REGISTRY_URL"));

        double latitude = Double.parseDouble(dotEnv.get("OPEN_METEO_LATITUDE", "31.2001"));
        double longitude = Double.parseDouble(dotEnv.get("OPEN_METEO_LONGITUDE", "29.9187"));
        Duration pollInterval = Duration.ofSeconds(
                Long.parseLong(dotEnv.get("OPEN_METEO_POLL_INTERVAL_SECONDS",
                        String.valueOf(DEFAULT_POLL_INTERVAL.toSeconds()))));

        log.info("Starting Open-Meteo adapter: stationId={}, lat={}, lon={}, pollInterval={}",
                OPEN_METEO_STATION_ID, latitude, longitude, pollInterval);

        StatusPublisher kafkaStatusPublisher = new KafkaStatusPublisher(config.defaultProducerProperties());

        OpenMeteoService openMeteoService = new OpenMeteoService(
                dotEnv.get("OPEN_METEO_URL"),
                latitude,
                longitude,
                OPEN_METEO_STATION_ID,
                pollInterval,
                kafkaStatusPublisher);

        List<ManagedService> services = List.of(openMeteoService);
        services.forEach(ManagedService::start);
        log.info("Open-Meteo adapter started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping Open-Meteo adapter");
            services.forEach(service -> {
                try {
                    service.stop();
                } catch (Exception e) {
                    log.error("Error stopping {}", service.getClass().getSimpleName(), e);
                }
            });
            log.info("Shutdown complete");
        }, "shutdown-hook"));
    }
}