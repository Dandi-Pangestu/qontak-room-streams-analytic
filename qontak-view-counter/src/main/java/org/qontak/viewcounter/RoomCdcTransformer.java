package org.qontak.viewcounter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.qontak.viewcounter.model.RoomCdc;
import org.qontak.viewcounter.model.RoomEvent;
import org.qontak.viewcounter.serdes.RoomCdcDeserializer;
import org.qontak.viewcounter.serdes.RoomEventSerializer;
import org.qontak.viewcounter.util.KafkaEnv;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class RoomCdcTransformer {

	/**
	 *
	 * @param args
	 * @throws Exception
	 *
	 * Savepoint path: s3://flink-data-1996/savepoints/room-cdc-transformer
	 * Latest savepoint:
	 * Latest checkpoint:
	 */
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<RoomCdc> cdcSource = KafkaSource.<RoomCdc>builder()
				.setBootstrapServers(KafkaEnv.Host())
				.setTopics("qontak_chat.public.rooms")
				.setGroupId("qontak-view-counter-room-cdc")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setDeserializer(new RoomCdcDeserializer())
				.build();

		DataStream<RoomCdc> cdcStream = env
				.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "RoomCDCSource");

		DataStream<RoomEvent> transformedStream = cdcStream
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

					return new RoomEvent(
							cdc.getAfter().getId(),
							cdc.getAfter().getName(),
							cdc.getAfter().getStatus(),
							cdc.getAfter().getType(),
							cdc.getAfter().getChannelIntegrationId(),
							cdc.getAfter().getAccountUniqId(),
							cdc.getAfter().getOrganizationId(),
							cdc.getAfter().getDivisionId(),
							cdc.getAfter().getIsBlocked(),
							LocalDateTime.ofInstant(Instant.ofEpochMilli(createdAtLong), ZoneId.of("UTC")),
							LocalDateTime.ofInstant(Instant.ofEpochMilli(updatedAtLong), ZoneId.of("UTC")),
							updatedAtLong
					);
				})
				.keyBy(RoomEvent::getId);

		KafkaSink<RoomEvent> kafkaSink = KafkaSink.<RoomEvent>builder()
				.setBootstrapServers(KafkaEnv.Host())
				.setRecordSerializer(new RoomEventSerializer("qontak_chat.public.room-events"))
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		transformedStream.sinkTo(kafkaSink);

		env.execute("Room CDC Tranformer Job");
	}
}
