package org.qontak.viewcounter.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.qontak.viewcounter.model.CustomViewEvent;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;

public class CustomViewEventSerializer implements KafkaRecordSerializationSchema<CustomViewEvent> {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setTimeZone(TimeZone.getTimeZone("UTC"));
    private final String topic;

    public CustomViewEventSerializer(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(CustomViewEvent event, KafkaSinkContext context, Long aLong) {
        try {
            byte[] key = String.valueOf(event.getId()).getBytes(StandardCharsets.UTF_8);
            byte[] value = objectMapper.writeValueAsBytes(event);
            return new ProducerRecord<>(topic, key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize custom view event", e);
        }
    }
}
