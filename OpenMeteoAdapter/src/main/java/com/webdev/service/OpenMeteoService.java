package com.webdev.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webdev.avro.AvroWeatherStatusMessage;
import com.webdev.infrastructure.StatusPublisher;
import com.webdev.mapper.OpenMeteoMessageMapper;
import com.webdev.record.OpenMeteoMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls Open-Meteo's current-weather endpoint for a fixed location and
 * publishes readins to kafka pipeline
 */

public class OpenMeteoService implements ManagedService{

    private static final Logger log = LoggerFactory.getLogger(OpenMeteoService.class);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private final String openMeteoBaseUrl;
    private final double latitude;
    private final double longitude;
    private final int stationId;
    private final Duration pollInterval;
    private final StatusPublisher statusPublisher;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private HttpClient httpClient;
    private long sequenceNumber = 0L;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "open-meteo-service"));

    private Future<?> task;

    public OpenMeteoService(String openMeteoBaseUrl,
                            double latitude,
                            double longitude,
                            int stationId,
                            Duration pollInterval,
                            StatusPublisher statusPublisher) {
        this.openMeteoBaseUrl = openMeteoBaseUrl;
        this.latitude = latitude;
        this.longitude = longitude;
        this.stationId = stationId;
        this.pollInterval = pollInterval;
        this.statusPublisher = statusPublisher;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("OpenMeteoService already running, ignoring start()");
            return;
        }
        httpClient = HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build();

        task = executor.submit(this::runLoop, "open-meteo-poller-" + stationId);
        log.info("OpenMeteoService started: stationId={}, pollInterval={}", stationId, pollInterval);
    }

    private void runLoop() {
        while (running.get()) {
            try {
                pollAndPublish();
            } catch (IOException e) {
                log.error("Open-Meteo poll failed, will retry next cycle", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // don't let one bad response or a mapping bug kill the poller
                log.error("Unexpected error polling Open-Meteo, will retry next cycle", e);
            }

            if (!sleepInterval()) {
                break;
            }
        }
        log.info("OpenMeteoService poll loop exiting for stationId={}", stationId);
    }

    private void pollAndPublish() throws IOException, InterruptedException {
        String requestUrl = buildRequestUrl();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(requestUrl))
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            log.error("Open-Meteo returned HTTP {}: {}", response.statusCode(), response.body());
            return;
        }

        OpenMeteoMessage msg = objectMapper.readValue(response.body(), OpenMeteoMessage.class);
        AvroWeatherStatusMessage avroMsg = OpenMeteoMessageMapper.toAvro(msg, ++sequenceNumber, stationId);

        System.out.println(avroMsg);
        statusPublisher.publish(avroMsg);
        log.debug("Published reading stationId={} sNo={}", stationId, sequenceNumber);
    }

    private String buildRequestUrl() {
        return openMeteoBaseUrl
                + "?latitude=" + latitude
                + "&longitude=" + longitude
                + "&current=temperature_2m,relative_humidity_2m,wind_speed_10m";
    }

    private boolean sleepInterval() {
        try {
            Thread.sleep(pollInterval);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping OpenMeteoService stationId={}", stationId);

        if (httpClient != null) {
            httpClient.close();
        }

        statusPublisher.close();

        try {
            task.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Exception while stopping parquet archiver task", e);
            throw new RuntimeException(e);
        } finally {
            executor.shutdownNow();
        }


        log.info("OpenMeteoService stopped");
    }
}