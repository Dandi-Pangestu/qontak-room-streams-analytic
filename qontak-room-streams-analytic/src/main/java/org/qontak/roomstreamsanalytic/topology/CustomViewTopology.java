package org.qontak.roomstreamsanalytic.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.qontak.roomstreamsanalytic.model.AggregatedCustomView;
import org.qontak.roomstreamsanalytic.model.CustomView;
import org.qontak.roomstreamsanalytic.model.EnrichedRoomCustomView;
import org.qontak.roomstreamsanalytic.model.RoomEvent;
import org.qontak.roomstreamsanalytic.serdes.JsonSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

// @Component
public class CustomViewTopology {

    // @Autowired
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, RoomEvent> roomEvents = streamsBuilder
                .table(
                        "room-streams-analytic-dev-room-events-changelog",
                        Consumed.with(Serdes.String(), JsonSerdes.RoomEvent())
                )
                .toStream()
                .selectKey((k, v) -> v.getOrganizationId());

        KTable<String, List<CustomView>> orgCustomViews = streamsBuilder
                .table(
                        "room-streams-analytic-dev-org-custom-views-changelog",
                        Consumed.with(Serdes.String(), JsonSerdes.CustomViewList())
                );

        KStream<String, EnrichedRoomCustomView> enrichedRoomCustomViewStreams = roomEvents
                .join(
                        orgCustomViews,
                        (roomEvent, customViews) -> customViews.stream()
                                .map(customView -> new EnrichedRoomCustomView(roomEvent, customView))
                                .collect(Collectors.toList()),
                        Joined.with(Serdes.String(), JsonSerdes.RoomEvent(), JsonSerdes.CustomViewList())
                )
                .flatMapValues(list -> list);

        KStream<String, EnrichedRoomCustomView> filteredRoomCustomViewStreams = enrichedRoomCustomViewStreams
                .filter((orgId, enriched) -> enriched.matchesCriteria());

        KTable<String, AggregatedCustomView> countPerView = filteredRoomCustomViewStreams
                .groupBy((orgId, enriched) -> enriched.getCustomView().getId(),
                        Grouped.with(Serdes.String(), JsonSerdes.EnrichedRoomCustomView()))
                .aggregate(
                        AggregatedCustomView::new,
                        (viewId, enriched, agg) -> agg.addRoom(enriched.getRoomEvent().getId()),
                        Materialized.<String, AggregatedCustomView, KeyValueStore<Bytes, byte[]>>as("custom-views-stat")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.AggregatedCustomView())
                );
    }
}
