package com.webdev;

// Kafka Stream Service to listen for incoming weather messages -> filter messages that have humidity more than 70% -> send it to a topic called rain

import com.webdev.service.KafkaStreamsRainDetectionService;
import io.github.cdimascio.dotenv.Dotenv;

public class Main {
    public static void main(String[] args) {

        Dotenv dotenv = Dotenv.load();
        KafkaStreamsRainDetectionService rainDetectionService = new KafkaStreamsRainDetectionService
                (dotenv.get("KAFKA_BOOTSTRAP_SERVER"));
        rainDetectionService.process();
    }
}