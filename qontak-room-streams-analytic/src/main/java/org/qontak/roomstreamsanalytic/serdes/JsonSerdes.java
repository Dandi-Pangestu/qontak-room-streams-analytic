package org.qontak.roomstreamsanalytic.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.qontak.roomstreamsanalytic.model.OrganizationRoomStatusKey;
import org.qontak.roomstreamsanalytic.model.RoomCDC;
import org.qontak.roomstreamsanalytic.model.RoomStatusCount;
import org.qontak.roomstreamsanalytic.model.RoomStatusEvent;

public class JsonSerdes {

    public static Serde<RoomCDC> RoomCDC() {
        JsonSerializer<RoomCDC> serializer = new JsonSerializer<>();
        JsonDeserializer<RoomCDC> deserializer = new JsonDeserializer<>(RoomCDC.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<RoomStatusEvent> RoomStatusEvent() {
        JsonSerializer<RoomStatusEvent> serializer = new JsonSerializer<>();
        JsonDeserializer<RoomStatusEvent> deserializer = new JsonDeserializer<>(RoomStatusEvent.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<OrganizationRoomStatusKey> OrganizationRoomStatusKey() {
        JsonSerializer<OrganizationRoomStatusKey> serializer = new JsonSerializer<>();
        JsonDeserializer<OrganizationRoomStatusKey> deserializer = new JsonDeserializer<>(OrganizationRoomStatusKey.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<RoomStatusCount> RoomStatusCount() {
        JsonSerializer<RoomStatusCount> serializer = new JsonSerializer<>();
        JsonDeserializer<RoomStatusCount> deserializer = new JsonDeserializer<>(RoomStatusCount.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
