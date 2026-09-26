package com.webdev.config;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Shared startup wiring for every service's Main: loads .env and sets the
 * system properties Avro needs before any KafkaConfig or serde is constructed.
 */
public class AppBootstrap {

    private AppBootstrap() {}

    public static Dotenv loadEnv() {
        System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "com.webdev.avro");
        return Dotenv.load();
    }
}