package com.webdev.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaConfig {

    private final String bootStrapServer;

    public KafkaConfig(String bootStrapServer) {
        this.bootStrapServer = bootStrapServer;
    }

    public Properties defaultStreamsConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detection-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        return config;
    }
}