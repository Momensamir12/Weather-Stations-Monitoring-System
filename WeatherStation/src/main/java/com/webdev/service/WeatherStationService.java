package com.webdev.service;

import com.webdev.infrastructure.StatusPublisher;
import com.webdev.utils.Clock;
import com.webdev.utils.RandomNumberGenerator;
import com.webdev.weathermessages.BatteryStatus;
import com.webdev.weathermessages.WeatherStatus;
import com.webdev.weathermessages.WeatherStatusMessage;

public class WeatherStationService {
    private final StatusPublisher publisher;
    private final RandomNumberGenerator randomNumberGenerator;
    private final Long stationId;
    private Long sequenceNumber;
    private final Clock clock;

    public WeatherStationService(Long stationId, StatusPublisher publisher, RandomNumberGenerator randomNumberGenerator,
                                 Clock clock) {
        this.publisher = publisher;
        this.stationId = stationId;
        this.sequenceNumber = 0L;
        this.randomNumberGenerator = randomNumberGenerator;
        this.clock = clock;
    }

    public WeatherStatusMessage generate()
    {
        int temperature = randomNumberGenerator.nextInt(-50, 100);
        int windSpeed = randomNumberGenerator.nextInt(0, 100);
        int humidity = randomNumberGenerator.nextInt(0, 100);
        BatteryStatus batteryStatus = getBatteryStatus();
        long currentTimestampMillis = clock.currentTimeMillis();

        WeatherStatus weatherStatus = new WeatherStatus(humidity, temperature, windSpeed);
        WeatherStatusMessage message = new WeatherStatusMessage(stationId, ++sequenceNumber, batteryStatus,
                currentTimestampMillis, weatherStatus);

        return message;
    }

    public BatteryStatus getBatteryStatus ()
    {
        double rand = randomNumberGenerator.nextDouble(0.0, 1.0);
        if(rand < 0.3)
            return BatteryStatus.LOW;
        if(rand < 0.7)
            return BatteryStatus.MID;

        return BatteryStatus.HIGH;
    }

    public void startStation() throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            WeatherStatusMessage message = generate();
            boolean drop = randomNumberGenerator.nextDouble(0.0, 1.0) < 0.1;
            if(!drop)
                publisher.publish(message);

            Thread.sleep(1000);
        }
    }
}
