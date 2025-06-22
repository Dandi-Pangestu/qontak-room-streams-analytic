package org.qontak.viewcounter.util;

import java.util.Optional;

public class KafkaEnv {

    public static String Host() {
        return Optional.ofNullable(System.getenv("KAFKA_HOST")).orElse("kafka:9092");
    }
}
