package com.webdev.serde;

import com.webdev.deserializer.JacksonDeserializer;
import com.webdev.serializer.JacksonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public JsonSerde(Class<T> targetType) {
        this.serializer = new JacksonSerializer<>();
        this.deserializer = new JacksonDeserializer<>(targetType);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
