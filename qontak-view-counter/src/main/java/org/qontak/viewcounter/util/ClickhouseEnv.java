package org.qontak.viewcounter.util;

import java.util.Optional;

public class ClickhouseEnv {

    public static String Host() {
        return Optional.ofNullable(System.getenv("CLICKHOUSE_HOST")).orElse("clickhouse");
    }

    public static String Username() {
        return Optional.ofNullable(System.getenv("CLICKHOUSE_USERNAME")).orElse("default");
    }

    public static String Password() {
        return Optional.ofNullable(System.getenv("CLICKHOUSE_PASSWORD")).orElse("password");
    }

    public static String Database() {
        return Optional.ofNullable(System.getenv("CLICKHOUSE_DATABASE")).orElse("default");
    }

    public static String Port() {
        return Optional.ofNullable(System.getenv("CLICKHOUSE_PORT")).orElse("8123");
    }

    public static String URL() {
        return String.format("jdbc:clickhouse://%s:%s/%s", Host(), Port(), Database());
    }
}
