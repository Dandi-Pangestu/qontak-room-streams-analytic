package org.qontak.roomstreamsanalytic.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.qontak.roomstreamsanalytic.model.RoomCDC;
import org.qontak.roomstreamsanalytic.model.RoomEvent;
import org.qontak.roomstreamsanalytic.processor.RoomCreatedAtTimestampExtractor;
import org.qontak.roomstreamsanalytic.serdes.JsonSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

// @Component
public class TransformRoomCdcTopology {

    // @Autowired
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, RoomCDC> roomCdcStreams = streamsBuilder
                .stream(
                        "qontak_chat.public.rooms",
                        Consumed.with(Serdes.String(), JsonSerdes.RoomCDC())
                                // Replace event time with room created at
                                .withTimestampExtractor(new RoomCreatedAtTimestampExtractor())
                )
                .peek((k, v) -> System.out.printf("\nPeek original room cdc events. Key: %s, Value: %s\n", k, v))
                .filter((k, v) -> {
                    ZonedDateTime threeMonthsAgo = ZonedDateTime.now(ZoneOffset.UTC).minusDays(90).truncatedTo(ChronoUnit.DAYS);
                    return v.getAfter().getCreatedAt() != null &&
                            v.getAfter().getCreatedAt().toInstant().toEpochMilli() >= threeMonthsAgo.toInstant().toEpochMilli();
                })
                .peek((k, v) -> System.out.printf("\nPeek filtered room cdc events. Key: %s, Value: %s\n", k, v));

        KTable<String, RoomEvent> roomEvents = roomCdcStreams
                .mapValues((v) -> new RoomEvent(
                        v.getAfter().getId(),
                        v.getAfter().getName(),
                        v.getAfter().getStatus(),
                        v.getAfter().getType(),
                        v.getAfter().getChannelIntegrationId(),
                        v.getAfter().getAccountUniqId(),
                        v.getAfter().getOrganizationId(),
                        v.getAfter().getDivisionId(),
                        v.getAfter().getIsBlocked(),
                        v.getAfter().getUpdatedAt().toInstant().toEpochMilli()
                ))
                .toTable(
                        Materialized.<String, RoomEvent, KeyValueStore<Bytes, byte[]>>as("room-events")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.RoomEvent())
                );
    }
}
