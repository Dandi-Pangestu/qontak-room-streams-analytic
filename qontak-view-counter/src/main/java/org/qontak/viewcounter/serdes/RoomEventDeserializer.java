package org.qontak.viewcounter.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.qontak.viewcounter.model.RoomEvent;

import java.io.IOException;
import java.util.TimeZone;

public class RoomEventDeserializer implements KafkaRecordDeserializationSchema<RoomEvent> {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setTimeZone(TimeZone.getTimeZone("UTC"));

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RoomEvent> out) throws IOException {
        if (record.value() != null) {
            RoomEvent roomEvent = objectMapper.readValue(record.value(), RoomEvent.class);
            out.collect(roomEvent);
        }
    }

    @Override
    public TypeInformation<RoomEvent> getProducedType() {
        return TypeInformation.of(RoomEvent.class);
    }
}
