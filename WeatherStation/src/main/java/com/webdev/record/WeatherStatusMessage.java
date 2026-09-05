package com.webdev.record;

public record WeatherStatusMessage(long stationId, long sequenceNumber, BatteryStatus batteryStatus,
                                   long statusTimeStamp, WeatherStatus weatherStatus) {
}
