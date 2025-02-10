package org.qontak.roomstreamsanalytic.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
@JsonIgnoreProperties
public class RoomCDC {

    private Before before;
    private After after;

    @Data
    @JsonIgnoreProperties
    public static class Before {

        private String id;
        private String status;
    }

    @Data
    @JsonIgnoreProperties
    public static class After {

        private String id;
        private String name;
        private String status;

        @JsonProperty("created_at")
        private ZonedDateTime createdAt;

        @JsonProperty("organization_id")
        private String organizationId;
    }

    @Override
    public String toString() {
        return "RoomCDC{" +
                "before=" + before +
                ", after=" + after +
                '}';
    }
}
