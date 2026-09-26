package com.webdev.service;

import com.webdev.avro.AvroRainMessage;
import com.webdev.avro.AvroWeatherStatusMessage;
import com.webdev.config.KafkaConfig;
import com.webdev.constants.Topics;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class KafkaStreamsRainDetectionService implements ManagedService{

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsRainDetectionService.class);

    private final KafkaConfig kafkaConfig;
    private volatile KafkaStreams stream;

    public KafkaStreamsRainDetectionService(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public void start() {

        StreamsBuilder builder = new StreamsBuilder();
        Properties props = kafkaConfig.defaultStreamsConfig();

        Map<String, String> serdeConfig = Map.of(
                "schema.registry.url",
                kafkaConfig.getSchemaRegistryUrl()
        );

        Serde<AvroWeatherStatusMessage> weatherStatusSerde =
                new SpecificAvroSerde<>();
        weatherStatusSerde.configure(serdeConfig, false);

        Serde<AvroRainMessage> rainMessageSerde =
                new SpecificAvroSerde<>();
        rainMessageSerde.configure(serdeConfig, false);

        KStream<String, AvroWeatherStatusMessage> weatherStream =
                builder.stream(
                        Topics.STATIONS_STATUS,
                        Consumed.with(
                                Serdes.String(),
                                weatherStatusSerde
                        )
                );

        KStream<String, AvroRainMessage> rainStream =
                weatherStream
                        .filter((key, value) ->
                                value != null &&
                                        value.getWeatherStatus().getHumidity() > 70
                        )
                        .mapValues(value ->
                                AvroRainMessage.newBuilder()
                                        .setStationId(value.getStationId())
                                        .setHumidity(
                                                value.getWeatherStatus().getHumidity()
                                        )
                                        .build()
                        );

        rainStream.to(
                Topics.RAIN_EVENTS,
                Produced.with(
                        Serdes.String(),
                        rainMessageSerde
                )
        );

        Topology topology = builder.build();

        stream = new KafkaStreams(topology, props);


        stream.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception in rain-detection Kafka Streams thread", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        stream.setStateListener((newState, oldState) -> {
            log.info("Rain detection stream state transition: {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                log.error("Rain detection stream entered ERROR state");
            }
        });

        stream.start();
        log.info("Rain detection stream started");
    }

    public void stop() {
        var s = stream;
        if (s != null) {
            log.info("Stopping rain detection stream");
            s.close();
            log.info("Rain detection stream stopped");
        }
    }
}