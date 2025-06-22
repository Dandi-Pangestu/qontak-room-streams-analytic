package org.qontak.roomstreamsanalytic.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.qontak.roomstreamsanalytic.model.OrganizationRoomStatusKey;
import org.qontak.roomstreamsanalytic.model.RoomStatusCount;
import org.qontak.roomstreamsanalytic.model.RoomEvent;
import org.qontak.roomstreamsanalytic.processor.RoomStatusChangeProcessor;
import org.qontak.roomstreamsanalytic.serdes.JsonSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

// @Component
public class RoomStatusTopology {

    // @Autowired
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, RoomEvent> roomEvents = streamsBuilder
                .table(
                        "room-streams-analytic-dev-room-events-changelog",
                        Consumed.with(Serdes.String(), JsonSerdes.RoomEvent())
                )
                .toStream();

        KGroupedStream<OrganizationRoomStatusKey, Long> grouped = roomEvents
                .processValues(RoomStatusChangeProcessor::new, Named.as("room-status-change-processor"), "room-status-store")
                .flatMap((roomId, roomStatusChange) -> {
                    List<KeyValue<OrganizationRoomStatusKey, Long>> updates = new ArrayList<>(2);

                    // Decrease count for the old status
                    if (roomStatusChange.getOldStatus() != null) {
                        updates.add(KeyValue.pair(
                                new OrganizationRoomStatusKey(roomStatusChange.getOrganizationId(), roomStatusChange.getOldStatus()),
                                -1L
                        ));
                    }

                    // Increase count for the new status
                    if (roomStatusChange.getNewStatus() != null) {
                        updates.add(KeyValue.pair(
                                new OrganizationRoomStatusKey(roomStatusChange.getOrganizationId(), roomStatusChange.getNewStatus()),
                                1L
                        ));
                    }

                    return updates;
                })
                .groupByKey(Grouped.with(JsonSerdes.OrganizationRoomStatusKey(), Serdes.Long()));

        KTable<Windowed<OrganizationRoomStatusKey>, Long> orgRoomStatusWindowed = grouped
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(90)).advanceBy(Duration.ofDays(1)))
                .aggregate(
                        () -> 0L,
                        (keu, newValue, aggValue) -> aggValue + newValue,
                        Materialized.with(JsonSerdes.OrganizationRoomStatusKey(), Serdes.Long())
                );

        orgRoomStatusWindowed
                .toStream()
                .filter((windowedKey, value) -> {
                    ZonedDateTime threeMonthsAgo = ZonedDateTime.now(ZoneOffset.UTC).minusDays(90).truncatedTo(ChronoUnit.DAYS);
                    Instant windowEndTime = windowedKey.window().endTime();
                    Instant windowStartTime = windowedKey.window().startTime();

                    return windowStartTime.toEpochMilli() >= threeMonthsAgo.toInstant().toEpochMilli() &&
                            windowEndTime.toEpochMilli() <= ZonedDateTime.now().toInstant().toEpochMilli();
                })
                .map((windowedKey, value) -> {
                    return KeyValue.pair(windowedKey.key(), new RoomStatusCount(windowedKey.key().getOrganizationId(), windowedKey.key().getRoomStatus(), value));
                })
                .peek((k, v) -> System.out.printf("\nPeek organization room status count events. Key: %s, Value: %s\n", k, v))
                .to(
                        "room-status-count",
                        Produced.with(JsonSerdes.OrganizationRoomStatusKey(), JsonSerdes.RoomStatusCount())
                );
    }
}
