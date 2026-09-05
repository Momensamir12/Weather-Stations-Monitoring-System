package com.webdev.service;

import com.webdev.config.KafkaConfig;
import com.webdev.constants.Topics;
import com.webdev.serde.JsonSerde;
import com.webdev.weathermessages.RainMessage;
import com.webdev.weathermessages.WeatherStatusMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class KafkaStreamsRainDetectionService {

    private final StreamsBuilder builder = new StreamsBuilder();
    private final KafkaConfig kafkaConfig;

    public KafkaStreamsRainDetectionService(String boostStrapServer) {
        kafkaConfig = new KafkaConfig(boostStrapServer);
    }

    public void process() {

        JsonSerde<WeatherStatusMessage> jsonWeatherSerde = new JsonSerde<>(WeatherStatusMessage.class);
        JsonSerde<RainMessage> jsonRainSerde = new JsonSerde<>(RainMessage.class);

        KStream<String, WeatherStatusMessage> weatherStream = builder.stream(
                Topics.STATIONS_STATUS,
                Consumed.with(Serdes.String(), jsonWeatherSerde)
        );

        KStream<String, RainMessage> rainStream = weatherStream
                .filter((key, value) -> value != null && value.weatherStatus().humidity() > 70)
                .mapValues(value ->
                        new RainMessage(value.stationId(), value.weatherStatus().humidity()));

        rainStream.to(Topics.RAIN_EVENTS, Produced.with(Serdes.String(), jsonRainSerde));

        Topology topology = builder.build();
        Properties props = kafkaConfig.defaultStreamsConfig();

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}