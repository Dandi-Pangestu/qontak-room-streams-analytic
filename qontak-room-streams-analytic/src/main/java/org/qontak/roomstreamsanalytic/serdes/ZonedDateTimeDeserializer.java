package org.qontak.roomstreamsanalytic.serdes;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ZonedDateTimeDeserializer implements JsonDeserializer<ZonedDateTime> {

    @Override
    public ZonedDateTime deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
        if (json == null || json.isJsonNull()) {
            return null;
        }

        try {
            String s = json.getAsString().trim();
            if (json.isJsonPrimitive() && s.isEmpty()) {
                return null;
            }

            long timestamp;
            if (s.length() > 13) {
                timestamp = json.getAsLong() / 1000;
            } else {
                timestamp = json.getAsLong();
            }

            return Instant.ofEpochMilli(timestamp).atZone(ZoneId.of("UTC"));
        } catch (Exception e) {
            throw new JsonParseException("Invalid timestamp format: " + json, e);
        }
    }
}
