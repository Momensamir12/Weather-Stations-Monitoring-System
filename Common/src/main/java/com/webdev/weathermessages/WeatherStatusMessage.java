package com.webdev.weathermessages;

public record WeatherStatusMessage(long stationId, long sequenceNumber, BatteryStatus batteryStatus,
                                   long statusTimeStamp, WeatherStatus weatherStatus) {
}
