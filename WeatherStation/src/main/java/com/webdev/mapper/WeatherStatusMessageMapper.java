package com.webdev.mapper;

import com.webdev.weathermessages.WeatherStatusMessage;

public final class WeatherStatusMessageMapper {

    private WeatherStatusMessageMapper() {
    }

    public static gen.AvroWeatherStatusMessage toAvro(WeatherStatusMessage message) {
        return gen.AvroWeatherStatusMessage.newBuilder()
                .setStationId(message.stationId())
                .setSequenceNumber(message.sequenceNumber())
                .setBatteryStatus(toAvroBatteryStatus(message.batteryStatus()))
                .setStatusTimeStamp(message.statusTimeStamp())
                .setWeatherStatus(toAvroWeatherStatus(message.weatherStatus()))
                .build();
    }

    private static gen.BatteryStatus toAvroBatteryStatus(
            com.webdev.weathermessages.BatteryStatus status) {

        return gen.BatteryStatus.valueOf(status.name());
    }

    private static gen.WeatherStatus toAvroWeatherStatus(
            com.webdev.weathermessages.WeatherStatus status) {

        return gen.WeatherStatus.newBuilder()
                .setHumidity(status.humidity())
                .setTemperature(status.temperature())
                .setWindSpeed(status.windSpeed())
                .build();
    }
}