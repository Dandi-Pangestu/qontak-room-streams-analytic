package org.qontak.roomstreamsanalytic.serdes;

import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.qontak.roomstreamsanalytic.model.*;

import java.util.List;

public class JsonSerdes {

    public static Serde<RoomCDC> RoomCDC() {
        JsonSerializer<RoomCDC> serializer = new JsonSerializer<>();
        JsonDeserializer<RoomCDC> deserializer = new JsonDeserializer<>(RoomCDC.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<RoomEvent> RoomEvent() {
        JsonSerializer<RoomEvent> serializer = new JsonSerializer<>();
        JsonDeserializer<RoomEvent> deserializer = new JsonDeserializer<>(RoomEvent.class);
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

    public static Serde<CustomViewCDC> CustomViewCDC() {
        JsonSerializer<CustomViewCDC> serializer = new JsonSerializer<>();
        JsonDeserializer<CustomViewCDC> deserializer = new JsonDeserializer<>(CustomViewCDC.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<CustomView> CustomView() {
        JsonSerializer<CustomView> serializer = new JsonSerializer<>();
        JsonDeserializer<CustomView> deserializer = new JsonDeserializer<>(CustomView.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<List<CustomView>> CustomViewList() {
        JsonSerializer<List<CustomView>> serializer = new JsonSerializer<>();
        JsonDeserializer<List<CustomView>> deserializer = new JsonDeserializer<>(new TypeToken<List<CustomView>>() {}.getType());
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<CustomViewWithId> CustomViewWithId() {
        JsonSerializer<CustomViewWithId> serializer = new JsonSerializer<>();
        JsonDeserializer<CustomViewWithId> deserializer = new JsonDeserializer<>(CustomViewWithId.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<EnrichedRoomCustomView> EnrichedRoomCustomView() {
        JsonSerializer<EnrichedRoomCustomView> serializer = new JsonSerializer<>();
        JsonDeserializer<EnrichedRoomCustomView> deserializer = new JsonDeserializer<>(EnrichedRoomCustomView.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<AggregatedCustomView> AggregatedCustomView() {
        JsonSerializer<AggregatedCustomView> serializer = new JsonSerializer<>();
        JsonDeserializer<AggregatedCustomView> deserializer = new JsonDeserializer<>(AggregatedCustomView.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<CustomViewPlainFilter> CustomViewPlainFilter() {
        JsonSerializer<CustomViewPlainFilter> serializer = new JsonSerializer<>();
        JsonDeserializer<CustomViewPlainFilter> deserializer = new JsonDeserializer<>(CustomViewPlainFilter.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
