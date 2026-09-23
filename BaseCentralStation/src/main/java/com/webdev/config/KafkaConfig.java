package com.webdev.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaConfig {

    private final String bootStrapServer;
    private final String schemaRegistryUrl;

    public KafkaConfig(String bootStrapServer, String schemaRegistryUrl) {
        this.bootStrapServer = bootStrapServer;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public Properties defaultStreamsConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "base-central-station");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());



        return config;
    }

    public Properties consumerConfig(String groupId) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put("specific.avro.reader", "true");
        return config;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }
}