package org.qontak.roomstreamsanalytic.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
public class KafkaStreamsConfig {

    @Autowired
    private Environment environment;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    @Order(2)
    @Primary
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        String applicationServerHost = environment.getProperty("kafka.streams.application-server-host");
        int applicationServerPort = Integer.parseInt(environment.getProperty("server.port"));

        Map<String, Object> props = Map.of(
                APPLICATION_ID_CONFIG, environment.getProperty("kafka.streams.application-id"),
                BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("kafka.streams.bootstrap-servers"),
                CACHE_MAX_BYTES_BUFFERING_CONFIG, 0,
                APPLICATION_SERVER_CONFIG, String.format("%s:%s", applicationServerHost, applicationServerPort),
                STATE_DIR_CONFIG, environment.getProperty("kafka.streams.state-dir"),
                DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class
        );

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    @Order(3)
    public StreamsBuilder streamsBuilder(KafkaStreamsConfiguration configuration) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.addStateStore(StoreConfig.RoomStatusStore());
        return streamsBuilder;
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder streamsBuilder, KafkaStreamsConfiguration config) {
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config.asProperties());
        kafkaStreams.start();
        return kafkaStreams;
    }

    @Bean
    @Order(1)
    public CreateTopicsResult createTopic() {
        String applicationId = environment.getProperty("kafka.streams.application-id");
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("kafka.streams.bootstrap-servers"));
        try (var adminClient = AdminClient.create(properties)) {
            return adminClient.createTopics(List.of(
                    new NewTopic(applicationId + "-room-status-store-changelog", 4, (short) 1)
                            .configs(Map.of(
                                    "cleanup.policy", "compact",
                                    "retention.ms", "7776000000"
                            )),
                    new NewTopic(applicationId + "-room-status-events-changelog", 4, (short) 1)
                            .configs(Map.of(
                                    "cleanup.policy", "compact",
                                    "retention.ms", "7776000000"
                            ))
            ));
        }
    }

    @Bean
    public HostInfo hostInfo() {
        String applicationServerHost = environment.getProperty("kafka.streams.application-server-host");
        int applicationServerPort = Integer.parseInt(environment.getProperty("server.port"));
        return new HostInfo(applicationServerHost, applicationServerPort);
    }
}
