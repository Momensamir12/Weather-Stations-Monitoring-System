package mapper;

import com.webdev.avro.AvroWeatherStatusMessage;
import com.webdev.avro.BatteryStatus;
import com.webdev.avro.WeatherStatus;
import com.webdev.weathermessages.WeatherStatusMessage;

public final class WeatherStatusMessageMapper {

    private WeatherStatusMessageMapper() {
    }

    public static AvroWeatherStatusMessage toAvro(WeatherStatusMessage message) {
        return AvroWeatherStatusMessage.newBuilder()
                .setStationId(message.stationId())
                .setSequenceNumber(message.sequenceNumber())
                .setBatteryStatus(toAvroBatteryStatus(message.batteryStatus()))
                .setStatusTimeStamp(message.statusTimeStamp())
                .setWeatherStatus(toAvroWeatherStatus(message.weatherStatus()))
                .build();
    }

    private static BatteryStatus toAvroBatteryStatus(
            com.webdev.weathermessages.BatteryStatus status) {

        return BatteryStatus.valueOf(status.name());
    }

    private static WeatherStatus toAvroWeatherStatus(
            com.webdev.weathermessages.WeatherStatus status) {

        return WeatherStatus.newBuilder()
                .setHumidity(status.humidity())
                .setTemperature(status.temperature())
                .setWindSpeed(status.windSpeed())
                .build();
    }
}