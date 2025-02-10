package org.qontak.roomstreamsanalytic.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.qontak.roomstreamsanalytic.model.RoomCDC;

public class RoomCreatedAtTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        RoomCDC roomCDC = (RoomCDC) record.value();
        if (roomCDC != null && roomCDC.getAfter() != null && roomCDC.getAfter().getCreatedAt() != null) {
            return roomCDC.getAfter().getCreatedAt().toInstant().toEpochMilli();
        }
        return partitionTime;
    }
}
