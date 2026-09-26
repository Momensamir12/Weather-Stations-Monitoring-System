package com.webdev;

import com.webdev.config.AppBootstrap;
import com.webdev.config.KafkaConfig;
import com.webdev.infrastructure.KafkaStatusPublisher;
import com.webdev.infrastructure.StatusPublisher;
import com.webdev.service.ManagedService;
import com.webdev.service.WeatherStationService;
import com.webdev.utils.Clock;
import com.webdev.utils.RandomNumberGenerator;
import com.webdev.utils.impl.RandomNumberGeneratorImpl;
import com.webdev.utils.impl.SystemClock;
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;


public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        Dotenv dotEnv = AppBootstrap.loadEnv();

        long stationId = Long.parseLong(dotEnv.get("STATION_ID"));

        KafkaConfig config = new KafkaConfig(
                dotEnv.get("KAFKA_BOOTSTRAP_SERVER"),
                dotEnv.get("SCHEMA_REGISTRY_URL"));

        log.info("Starting weather station: stationId={}", stationId);

        StatusPublisher kafkaStatusPublisher = new KafkaStatusPublisher(config.defaultProducerProperties());
        RandomNumberGenerator randomNumberGenerator = new RandomNumberGeneratorImpl();
        Clock clock = new SystemClock();

        WeatherStationService weatherStationService = new WeatherStationService(
                stationId, kafkaStatusPublisher, randomNumberGenerator, clock);

        List<ManagedService> services = new ArrayList<>();
        services.add(weatherStationService);

        services.forEach(ManagedService::start);
        log.info("All services started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping services");

            List<ManagedService> reversed = new ArrayList<>(services);
            Collections.reverse(reversed);

            reversed.forEach(managedService -> {
                try {
                    managedService.stop();
                } catch (Exception e) {
                    log.error("Error stopping {}", managedService.getClass().getSimpleName(), e);
                }
            });

            log.info("Shutdown complete");
        }, "shutdown-hook"));
    }
}