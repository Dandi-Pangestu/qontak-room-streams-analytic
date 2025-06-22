package org.qontak.roomstreamsanalytic.topology;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.qontak.roomstreamsanalytic.model.CustomView;
import org.qontak.roomstreamsanalytic.model.CustomViewCDC;
import org.qontak.roomstreamsanalytic.model.CustomViewFilter;
import org.qontak.roomstreamsanalytic.model.CustomViewWithId;
import org.qontak.roomstreamsanalytic.serdes.JsonSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// @Component
public class GroupedCustomViewTopology {

    // @Autowired
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, CustomViewCDC> customViewCdcStreams = streamsBuilder
                .stream(
                        "qontak_chat.public.custom_views",
                        Consumed.with(Serdes.String(), JsonSerdes.CustomViewCDC())
                );

        KStream<String, CustomViewWithId> customViewWithIdStreams = customViewCdcStreams
                .mapValues((v) -> {
                    if (v.getAfter() == null) { // Handle deleted custom view
                        return new CustomViewWithId(
                                v.getBefore().getId(),
                                v.getBefore().getOrganizationId(),
                                null // Indicating deleted custom view
                        );
                    }

                    List<CustomViewFilter> filters = new Gson()
                            .fromJson(v.getAfter().getFilters(), new TypeToken<List<CustomViewFilter>>() {}.getType());

                    return new CustomViewWithId(
                            v.getAfter().getId(),
                            v.getAfter().getOrganizationId(),
                            new CustomView(
                                    v.getAfter().getId(),
                                    v.getAfter().getName(),
                                    v.getAfter().getOrganizationId(),
                                    filters
                            )
                    );
                });

        KStream<String, CustomViewWithId> rekeyedCustomViewWithIdStreams = customViewWithIdStreams
                .selectKey((k, v) -> v.getOrganizationId());

        KTable<String, List<CustomView>> orgCustomViews = rekeyedCustomViewWithIdStreams
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.CustomViewWithId()))
                .aggregate(
                        ArrayList::new,
                        (orgId, customViewWithId, existingCustomViews) -> {
                            if (customViewWithId.getCustomView() == null) {
                                return existingCustomViews
                                        .stream()
                                        .filter(customView -> !customView.getId().equals(customViewWithId.getId()))
                                        .collect(Collectors.toList());
                            } else {
                                List<CustomView> updatedCustomViews = new ArrayList<>(existingCustomViews);
                                updatedCustomViews.removeIf(v -> v.getId().equals(customViewWithId.getId()));
                                updatedCustomViews.add(customViewWithId.getCustomView());
                                return updatedCustomViews;
                            }
                        },
                        Materialized.<String, List<CustomView>, KeyValueStore<Bytes, byte[]>>as("org-custom-views")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.CustomViewList())
                );
    }
}
