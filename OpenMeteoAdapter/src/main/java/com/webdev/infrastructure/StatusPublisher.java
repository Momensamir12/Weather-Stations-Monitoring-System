package com.webdev.infrastructure;


import com.webdev.avro.AvroWeatherStatusMessage;

public interface StatusPublisher extends AutoCloseable{
    void publish (AvroWeatherStatusMessage message);
    @Override
    void close();
}
