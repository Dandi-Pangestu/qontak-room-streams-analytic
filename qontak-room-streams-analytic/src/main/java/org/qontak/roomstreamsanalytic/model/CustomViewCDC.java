package org.qontak.roomstreamsanalytic.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.List;

@Data
@JsonIgnoreProperties
public class CustomViewCDC {

    private Before before;
    private After after;

    @Data
    @JsonIgnoreProperties
    public static class Before {
        private String id;

        @JsonProperty("organization_id")
        private String organizationId;
    }

    @Data
    @JsonIgnoreProperties
    public static class After {
        private String id;
        private String name;
        private String filters;

        @JsonProperty("organization_id")
        private String organizationId;

        @JsonProperty("updated_at")
        private ZonedDateTime updatedAt;
    }
}
