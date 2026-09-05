package com.webdev.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class JacksonDeserializer <T> implements Deserializer<T> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> targetType;

    public JacksonDeserializer (Class<T> targetType)
    {
        this.targetType = targetType;
    }
    @Override
    public T deserialize(String topic, byte[] data) {

        try {
            return mapper.readValue(data, targetType);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error deserializing message from topic ");
        }
    }
}
