package com.webdev.service;

import com.webdev.Bitcask;
import com.webdev.config.KafkaConfig;
import com.webdev.constants.Topics;
import gen.AvroWeatherStatusMessage;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BitcaskConsumerService {

    private final Logger log = LoggerFactory.getLogger(BitcaskConsumerService.class);
    private final Bitcask bitcask;
    private final KafkaConfig kafkaConfig;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile KafkaConsumer<String, AvroWeatherStatusMessage> consumer;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "bitcask writer"));

    private Future<?> task;

    public BitcaskConsumerService(Bitcask bitcask, KafkaConfig kafkaConfig) {
        this.bitcask = bitcask;
        this.kafkaConfig = kafkaConfig;
    }

    public void start() {
        running.set(true);
        task = executor.submit(this::runLoop);
    }

    private void runLoop() {

        try (var c = new KafkaConsumer<String, AvroWeatherStatusMessage>(
                kafkaConfig.consumerConfig("bitcask-writer"))) {
            this.consumer = c;
            c.subscribe(List.of(Topics.STATIONS_STATUS));
            while (running.get()) {
                var records = c.poll(Duration.ofMillis(500));
                for (var r : records) {

                    log.debug("Consumed key={} value={}", r.key(), r.value());

                    bitcask.put(r.key().getBytes(StandardCharsets.UTF_8),
                            r.value().toString().getBytes(StandardCharsets.UTF_8));
                }
                if (!records.isEmpty()) {
                    c.commitSync();
                }
            }
        } catch (WakeupException _) {

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() throws Exception {

        running.set(false);
        var c = consumer;
        if (c != null) c.wakeup();

        try {
            task.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException _) {

        } catch (TimeoutException | InterruptedException e) {
            task.cancel(true);
        } finally {
            executor.shutdownNow();
            bitcask.close();
        }
    }
}