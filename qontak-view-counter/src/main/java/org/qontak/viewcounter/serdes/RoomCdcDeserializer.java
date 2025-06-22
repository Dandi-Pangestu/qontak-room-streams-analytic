package org.qontak.viewcounter.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.qontak.viewcounter.model.RoomCdc;

import java.io.IOException;
import java.util.TimeZone;

public class RoomCdcDeserializer implements KafkaRecordDeserializationSchema<RoomCdc> {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setTimeZone(TimeZone.getTimeZone("UTC"));

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RoomCdc> out) throws IOException {
        if (record.value() != null) {
            RoomCdc roomCdc = objectMapper.readValue(record.value(), RoomCdc.class);
            out.collect(roomCdc);
        }
    }

    @Override
    public TypeInformation<RoomCdc> getProducedType() {
        return TypeInformation.of(RoomCdc.class);
    }
}
