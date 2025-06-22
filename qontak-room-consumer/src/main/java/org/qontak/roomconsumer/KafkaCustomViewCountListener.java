package org.qontak.roomconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.qontak.roomconsumer.model.AggregatedViewCount;
import org.qontak.roomconsumer.model.OrganizationRoomStatusKey;
import org.qontak.roomconsumer.model.RoomStatusCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaCustomViewCountListener {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "qontak_chat.public.aggregated-view-count", groupId = "qontak-consumer-aggregated-view-count")
    public void listen(@Header(KafkaHeaders.RECEIVED_KEY) String k, String v) {
        System.out.println("Received from Kafka: " + v);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            AggregatedViewCount aggregatedViewCount = objectMapper.readValue(v, AggregatedViewCount.class);
            String destination = "/topic/custom-view-count/" + aggregatedViewCount.getViewId();
            messagingTemplate.convertAndSend(destination, aggregatedViewCount);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
