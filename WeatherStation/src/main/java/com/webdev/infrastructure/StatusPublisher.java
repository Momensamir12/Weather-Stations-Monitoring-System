package com.webdev.infrastructure;

import com.webdev.record.WeatherStatusMessage;

public interface StatusPublisher extends AutoCloseable{
    void publish (WeatherStatusMessage message);
    @Override
    void close();
}
