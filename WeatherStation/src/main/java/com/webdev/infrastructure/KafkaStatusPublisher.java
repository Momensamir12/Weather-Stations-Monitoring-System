package com.webdev.infrastructure;

import com.webdev.record.WeatherStatusMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaStatusPublisher implements StatusPublisher {

    private static final String TOPIC = "station-status";

    private final KafkaProducer<String, WeatherStatusMessage> producer;

    public KafkaStatusPublisher(Properties properties) {
        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public void publish(WeatherStatusMessage message) {
        ProducerRecord<String, WeatherStatusMessage> record =
                new ProducerRecord<>(TOPIC, message);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                // proper logging here
                System.err.println(
                        "Failed to publish weather status: " +
                                exception.getMessage()
                );
            }
        });
        System.out.println(message);
    }

    @Override
    public void close() {
        producer.close();
    }
}