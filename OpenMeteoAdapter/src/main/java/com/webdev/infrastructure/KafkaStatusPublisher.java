package com.webdev.infrastructure;

import com.webdev.avro.AvroWeatherStatusMessage;
import com.webdev.constants.Topics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStatusPublisher implements StatusPublisher {

    private final KafkaProducer<String, AvroWeatherStatusMessage> producer;
    private static final Logger log = LoggerFactory.getLogger(KafkaStatusPublisher.class);

    public KafkaStatusPublisher(Properties properties) {
        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public void publish(AvroWeatherStatusMessage message) {

        ProducerRecord<String, AvroWeatherStatusMessage> record =
                new ProducerRecord<>(Topics.STATIONS_STATUS, String.valueOf(message.getStationId()), message);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error(
                        "Failed to publish weather status to Kafka", exception);
            }
        });
        log.debug(message.toString());
    }

    @Override
    public void close() {
        producer.close();
    }
}