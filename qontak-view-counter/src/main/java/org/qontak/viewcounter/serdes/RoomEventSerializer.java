package org.qontak.viewcounter.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.qontak.viewcounter.model.RoomEvent;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;

public class RoomEventSerializer implements KafkaRecordSerializationSchema<RoomEvent> {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setTimeZone(TimeZone.getTimeZone("UTC"));
    private final String topic;

    public RoomEventSerializer(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(RoomEvent roomEvent, KafkaSinkContext context, Long aLong) {
        try {
            byte[] key = String.valueOf(roomEvent.getId()).getBytes(StandardCharsets.UTF_8);
            byte[] value = objectMapper.writeValueAsBytes(roomEvent);
            return new ProducerRecord<>(topic, key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize room event", e);
        }
    }
}
