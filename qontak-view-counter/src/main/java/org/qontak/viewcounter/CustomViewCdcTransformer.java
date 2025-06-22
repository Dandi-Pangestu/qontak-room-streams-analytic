package org.qontak.viewcounter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.qontak.viewcounter.model.*;
import org.qontak.viewcounter.serdes.CustomViewCdcDeserializer;
import org.qontak.viewcounter.serdes.CustomViewEventSerializer;
import org.qontak.viewcounter.util.KafkaEnv;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class CustomViewCdcTransformer {

	/**
     *
	 * @param args
	 * @throws Exception
	 *
	 * Savepoint path: s3://flink-data-1996/savepoints/custom-view-cdc-transformer
	 * Latest savepoint:
	 * Latest checkpoint:
	 */
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<CustomViewCdc> cdcSource = KafkaSource.<CustomViewCdc>builder()
				.setBootstrapServers(KafkaEnv.Host())
				.setTopics("qontak_chat.public.custom_views")
				.setGroupId("qontak-view-counter-custom-view-cdc")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setDeserializer(new CustomViewCdcDeserializer())
				.build();

		DataStream<CustomViewCdc> cdcStream = env
				.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "CustomViewCDCSource");

		DataStream<CustomViewEvent> transformedStream = cdcStream
				.map(cdc -> {
					long createdAtLong;
					long updatedAtLong;

					if (String.valueOf(cdc.getAfter().getCreatedAt()).length() > 13) {
						createdAtLong = cdc.getAfter().getCreatedAt() / 1000;
					} else {
						createdAtLong = cdc.getAfter().getCreatedAt();
					}

					if (String.valueOf(cdc.getAfter().getUpdatedAt()).length() > 13) {
						updatedAtLong = cdc.getAfter().getUpdatedAt() / 1000;
					} else {
						updatedAtLong = cdc.getAfter().getUpdatedAt();
					}

					return new CustomViewEvent(
							cdc.getAfter().getId(),
							cdc.getAfter().getName(),
							cdc.getAfter().getOrganizationId(),
							cdc.getAfter().getFilters(),
							LocalDateTime.ofInstant(Instant.ofEpochMilli(createdAtLong), ZoneId.of("UTC")),
							LocalDateTime.ofInstant(Instant.ofEpochMilli(updatedAtLong), ZoneId.of("UTC")),
							updatedAtLong
					);
				})
				.keyBy(CustomViewEvent::getId);

		KafkaSink<CustomViewEvent> kafkaSink = KafkaSink.<CustomViewEvent>builder()
				.setBootstrapServers(KafkaEnv.Host())
				.setRecordSerializer(new CustomViewEventSerializer("qontak_chat.public.custom-view-events"))
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		transformedStream.sinkTo(kafkaSink);

		env.execute("Custom View CDC Tranformer Job");
	}
}
