package com.webdev.mapper;

import com.webdev.avro.AvroWeatherStatusMessage;
import com.webdev.avro.BatteryStatus;
import com.webdev.avro.WeatherStatus;
import com.webdev.record.OpenMeteoMessage;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public final class OpenMeteoMessageMapper {

    private OpenMeteoMessageMapper() {
    }

    public static AvroWeatherStatusMessage toAvro(OpenMeteoMessage message, long sequenceNumber, long stationId) {

        return AvroWeatherStatusMessage.newBuilder()
                .setStationId(stationId)
                .setSequenceNumber(sequenceNumber)
                .setBatteryStatus(BatteryStatus.NOT_AVAILABLE)
                .setStatusTimeStamp(toTimestamp(message.current().time()))
                .setWeatherStatus(toAvroWeatherStatus(message))
                .build();
    }

    private static WeatherStatus toAvroWeatherStatus(
            OpenMeteoMessage message) {

        OpenMeteoMessage.Current current = message.current();

        return WeatherStatus.newBuilder()
                .setHumidity(current.relative_humidity_2m())
                .setTemperature(current.temperature_2m())
                .setWindSpeed(current.wind_speed_10m())
                .build();
    }

    private static long toTimestamp(String time) {
        return LocalDateTime.parse(time)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
    }
}