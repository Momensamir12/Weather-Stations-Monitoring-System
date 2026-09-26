package com.webdev.service;

import com.webdev.infrastructure.StatusPublisher;
import com.webdev.utils.Clock;
import com.webdev.utils.RandomNumberGenerator;
import com.webdev.weathermessages.BatteryStatus;
import com.webdev.weathermessages.WeatherStatus;
import com.webdev.weathermessages.WeatherStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class WeatherStationService implements ManagedService {

    private static final Logger log = LoggerFactory.getLogger(WeatherStationService.class);

    private static final double BATTERY_LOW_THRESHOLD = 0.3;
    private static final double BATTERY_MID_THRESHOLD = 0.7;
    private static final double DROP_RATE = 0.1;
    private static final long EMIT_INTERVAL_MILLIS = 1000;
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);

    private final StatusPublisher publisher;
    private final RandomNumberGenerator randomNumberGenerator;
    private final Long stationId;
    private final Clock clock;
    private long sequenceNumber = 0L;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "weather-station-" + stationIdPlaceholder()));
    private Future<?> task;

    public WeatherStationService(Long stationId, StatusPublisher publisher,
                                 RandomNumberGenerator randomNumberGenerator, Clock clock) {
        this.publisher = publisher;
        this.stationId = stationId;
        this.randomNumberGenerator = randomNumberGenerator;
        this.clock = clock;
    }

    private String stationIdPlaceholder() {
        // executor field initializer runs before the constructor body assigns
        // stationId's final value in some field orderings — see note below
        return String.valueOf(stationId);
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("WeatherStationService[{}] already running, ignoring start()", stationId);
            return;
        }
        task = executor.submit(this::runLoop);
        log.info("WeatherStationService[{}] started", stationId);
    }

    private void runLoop() {
        try {
            while (running.get()) {
                WeatherStatusMessage message = generate();
                boolean drop = randomNumberGenerator.nextDouble(0.0, 1.0) < DROP_RATE;

                if (!drop) {
                    try {
                        publisher.publish(message);
                        log.debug("Published stationId={} sNo={}", stationId, message.sequenceNumber());
                    } catch (Exception e) {
                        // one bad publish shouldn't take down the whole station —
                        // log it and keep emitting on the next tick
                        log.error("Failed to publish stationId={} sNo={}", stationId, message.sequenceNumber(), e);
                    }
                } else {
                    log.debug("Dropped message stationId={} sNo={}", stationId, message.sequenceNumber());
                }

                Thread.sleep(EMIT_INTERVAL_MILLIS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("WeatherStationService[{}] loop interrupted, exiting", stationId);
        }
    }

    public WeatherStatusMessage generate() {
        int temperature = randomNumberGenerator.nextInt(-50, 100);
        int windSpeed = randomNumberGenerator.nextInt(0, 100);
        int humidity = randomNumberGenerator.nextInt(0, 100);
        BatteryStatus batteryStatus = getBatteryStatus();
        long currentTimestampMillis = clock.currentTimeMillis();

        WeatherStatus weatherStatus = new WeatherStatus(humidity, temperature, windSpeed);
        return new WeatherStatusMessage(stationId, ++sequenceNumber, batteryStatus,
                currentTimestampMillis, weatherStatus);
    }

    public BatteryStatus getBatteryStatus() {
        double rand = randomNumberGenerator.nextDouble(0.0, 1.0);
        if (rand < BATTERY_LOW_THRESHOLD) return BatteryStatus.LOW;
        if (rand < BATTERY_MID_THRESHOLD) return BatteryStatus.MID;
        return BatteryStatus.HIGH;
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping WeatherStationService[{}]", stationId);

        task.cancel(true);   // interrupts the Thread.sleep in runLoop immediately

        try {
            task.get(STOP_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } catch (CancellationException e) {
            // expected: task.cancel(true) causes get() to throw this on clean interrupt-exit
        } catch (ExecutionException e) {
            log.error("WeatherStationService[{}] loop terminated with an exception", stationId, e.getCause());
        } catch (TimeoutException | InterruptedException e) {
            log.warn("WeatherStationService[{}] did not stop within {}", stationId, STOP_TIMEOUT);
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdownNow();
            publisher.close();
            log.info("WeatherStationService[{}] stopped", stationId);
        }
    }
}