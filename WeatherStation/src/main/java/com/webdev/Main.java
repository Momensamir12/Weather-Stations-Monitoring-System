package com.webdev;

import com.webdev.config.KafkaConfig;
import com.webdev.infrastructure.KafkaStatusPublisher;
import com.webdev.infrastructure.StatusPublisher;
import com.webdev.service.WeatherStationService;
import com.webdev.utils.Clock;
import com.webdev.utils.RandomNumberGenerator;
import com.webdev.utils.impl.RandomNumberGeneratorImpl;
import com.webdev.utils.impl.SystemClock;
import io.github.cdimascio.dotenv.Dotenv;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws InterruptedException {

        Dotenv dotEnv = Dotenv.load();
        String kafkaBootStrapServer = dotEnv.get("KAFKA_BOOTSTRAP_SERVER");
        long stationId = Long.parseLong(dotEnv.get("STATION_ID"));

        KafkaConfig config = new KafkaConfig(kafkaBootStrapServer);
        StatusPublisher kafkaStatusPublisher = new KafkaStatusPublisher(config.defaultProducerProperties());
        RandomNumberGenerator randomNumberGenerator = new RandomNumberGeneratorImpl();
        Clock clock = new SystemClock();

        WeatherStationService weatherStationService = new WeatherStationService(stationId, kafkaStatusPublisher,
                randomNumberGenerator, clock);

        weatherStationService.startStation();

        }
    }
