package com.webdev.record;

public record OpenMeteoMessage(
        double latitude,
        double longitude,
        double generationtime_ms,
        int utc_offset_seconds,
        String timezone,
        String timezone_abbreviation,
        double elevation,
        CurrentUnits current_units,
        Current current
) {

    public record CurrentUnits(
            String time,
            String interval,
            String temperature_2m,
            String relative_humidity_2m,
            String wind_speed_10m
    ) {}

    public record Current(
            String time,
            int interval,
            double temperature_2m,
            int relative_humidity_2m,
            double wind_speed_10m
    ) {}
}