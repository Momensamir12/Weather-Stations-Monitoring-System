package com.webdev.service;

import com.webdev.record.BatteryStatus;
import com.webdev.record.WeatherStatusMessage;
import com.webdev.utils.Clock;
import com.webdev.utils.RandomNumberGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WeatherStationServiceTest {
    private RandomNumberGenerator mockRandom;
    private Clock mockClock;
    private WeatherStationService service;

    @BeforeEach
    void setUp()
    {
        mockRandom = mock(RandomNumberGenerator.class);
        mockClock = mock(Clock.class);
        service = new WeatherStationService(1L, null, mockRandom, mockClock);
    }

    @Test
    void generate_createsMessagesWithExpectedValues()
    {
        when(mockRandom.nextInt(-50, 100)).thenReturn(25);
        when(mockRandom.nextInt(0, 100)).thenReturn(40,60);
        when(mockRandom.nextDouble(anyDouble(), anyDouble())).thenReturn(0.5);
        when(mockClock.currentTimeMillis()).thenReturn(123456789L);
        WeatherStatusMessage message = service.generate();

        assertEquals(1L, message.stationId());
        assertEquals(1L, message.sequenceNumber());
        assertEquals(123456789L, message.statusTimeStamp());
        assertEquals(25, message.weatherStatus().temperature());
        assertEquals(40, message.weatherStatus().windSpeed());
        assertEquals(60, message.weatherStatus().humidity());
        assertEquals(BatteryStatus.MID, message.batteryStatus());
    }
    @Test
    void generate_incrementsSequenceNumber() {
        service.generate();
        service.generate();
        WeatherStatusMessage msg = service.generate();
        assertEquals(3L, msg.sequenceNumber());
    }

    @Test
    void getBatteryStatus_returnsCorrectEnum() {
        when(mockRandom.nextDouble(anyDouble(), anyDouble())).thenReturn(0.1);
        assertEquals(BatteryStatus.LOW, service.getBatteryStatus());

        when(mockRandom.nextDouble(anyDouble(), anyDouble())).thenReturn(0.5);
        assertEquals(BatteryStatus.MID, service.getBatteryStatus());

        when(mockRandom.nextDouble(anyDouble(), anyDouble())).thenReturn(0.9);
        assertEquals(BatteryStatus.HIGH, service.getBatteryStatus());
    }
}