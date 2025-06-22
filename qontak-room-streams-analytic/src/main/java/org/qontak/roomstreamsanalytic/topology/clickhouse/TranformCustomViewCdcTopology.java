package org.qontak.roomstreamsanalytic.topology.clickhouse;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.qontak.roomstreamsanalytic.model.*;
import org.qontak.roomstreamsanalytic.serdes.JsonSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TranformCustomViewCdcTopology {

    @Autowired
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, CustomViewCDC> customViewCdcStreams = streamsBuilder
                .stream(
                        "qontak_chat.public.custom_views",
                        Consumed.with(Serdes.String(), JsonSerdes.CustomViewCDC())
                );

        customViewCdcStreams
                .mapValues((v) -> new CustomViewPlainFilter(
                        v.getAfter().getId(),
                        v.getAfter().getName(),
                        v.getAfter().getOrganizationId(),
                        v.getAfter().getFilters(),
                        v.getAfter().getUpdatedAt().toInstant().toEpochMilli()
                ))
                .to(
                        "custom-views",
                        Produced.with(Serdes.String(), JsonSerdes.CustomViewPlainFilter())
                );
    }
}
