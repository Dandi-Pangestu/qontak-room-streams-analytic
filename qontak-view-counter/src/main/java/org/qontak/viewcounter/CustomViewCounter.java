package org.qontak.viewcounter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.qontak.viewcounter.model.AggregatedViewCount;
import org.qontak.viewcounter.model.CustomViewEvent;
import org.qontak.viewcounter.model.RoomEvent;
import org.qontak.viewcounter.operator.AggregatedViewCounter;
import org.qontak.viewcounter.serdes.CustomViewEventDeserializer;
import org.qontak.viewcounter.serdes.AggregatedViewCountSerializer;
import org.qontak.viewcounter.serdes.RoomEventDeserializer;
import org.qontak.viewcounter.util.KafkaEnv;

public class CustomViewCounter {

	/**
	 *
	 * @param args
	 * @throws Exception
	 *
	 * Savepoint path: s3://flink-data-1996/savepoints/custom-view-counter
	 * Latest savepoint:
	 * Latest checkpoint:
	 */
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<RoomEvent> roomEventSource = KafkaSource.<RoomEvent>builder()
				.setBootstrapServers(KafkaEnv.Host())
				.setTopics("qontak_chat.public.room-events")
				.setGroupId("qontak-view-counter-room-event-view-counter")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setDeserializer(new RoomEventDeserializer())
				.build();

		KafkaSource<CustomViewEvent> viewEventSource = KafkaSource.<CustomViewEvent>builder()
				.setBootstrapServers(KafkaEnv.Host())
				.setTopics("qontak_chat.public.custom-view-events")
				.setGroupId("qontak-view-counter-view-event-view-counter")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setDeserializer(new CustomViewEventDeserializer())
				.build();

		DataStream<RoomEvent> roomEventStream = env
				.fromSource(roomEventSource, WatermarkStrategy.noWatermarks(), "RoomEventSource")
				.keyBy(RoomEvent::getOrganizationId);

		DataStream<CustomViewEvent> viewEventStream = env
				.fromSource(viewEventSource, WatermarkStrategy.noWatermarks(), "CustomViewEventSource")
				.keyBy(CustomViewEvent::getOrganizationId);

		DataStream<AggregatedViewCount> aggregatedViewCountStream = roomEventStream
				.keyBy(RoomEvent::getOrganizationId)
				.connect(viewEventStream)
				.process(new AggregatedViewCounter());

		KafkaSink<AggregatedViewCount> kafkaSink = KafkaSink.<AggregatedViewCount>builder()
				.setBootstrapServers(KafkaEnv.Host())
				.setRecordSerializer(new AggregatedViewCountSerializer("qontak_chat.public.aggregated-view-count"))
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		aggregatedViewCountStream.sinkTo(kafkaSink);

		env.execute("Custom View Counter Job");
	}
}
