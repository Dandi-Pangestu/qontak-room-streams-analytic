package org.qontak.viewcounter.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.qontak.viewcounter.model.CustomViewCdc;

import java.io.IOException;
import java.util.TimeZone;

public class CustomViewCdcDeserializer implements KafkaRecordDeserializationSchema<CustomViewCdc> {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setTimeZone(TimeZone.getTimeZone("UTC"));

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CustomViewCdc> out) throws IOException {
        if (record.value() != null) {
            CustomViewCdc customViewCdc = objectMapper.readValue(record.value(), CustomViewCdc.class);
            out.collect(customViewCdc);
        }
    }

    @Override
    public TypeInformation<CustomViewCdc> getProducedType() {
        return TypeInformation.of(CustomViewCdc.class);
    }
}
