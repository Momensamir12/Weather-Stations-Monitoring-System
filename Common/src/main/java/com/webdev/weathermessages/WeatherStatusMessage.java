package com.webdev.weathermessages;

public record WeatherStatusMessage(
        Long stationId,
        Long sequenceNumber,
        BatteryStatus batteryStatus,
        long statusTimeStamp,
        WeatherStatus weatherStatus
) {
}