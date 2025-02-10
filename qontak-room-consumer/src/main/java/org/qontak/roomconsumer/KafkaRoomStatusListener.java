package org.qontak.roomconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.qontak.roomconsumer.model.OrganizationRoomStatusKey;
import org.qontak.roomconsumer.model.RoomStatusCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaRoomStatusListener {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "room-status-count", groupId = "room-status-count")
    public void listen(@Header(KafkaHeaders.RECEIVED_KEY) String k, String v) {
        System.out.println("Received from Kafka: " + v);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            RoomStatusCount roomStatus = objectMapper.readValue(v, RoomStatusCount.class);
            OrganizationRoomStatusKey key = objectMapper.readValue(k, OrganizationRoomStatusKey.class);
            String destination = "/topic/room-status-count/" + key.getOrganizationId();
            messagingTemplate.convertAndSend(destination, roomStatus);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
