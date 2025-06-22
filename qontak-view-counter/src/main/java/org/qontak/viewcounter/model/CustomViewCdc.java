package org.qontak.viewcounter.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomViewCdc {

    private Before before;
    private After after;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Before {
        private String id;

        @JsonProperty("organization_id")
        private String organizationId;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class After {
        private String id;
        private String name;
        private String filters;

        @JsonProperty("organization_id")
        private String organizationId;

        @JsonProperty("created_at")
        private long createdAt;

        @JsonProperty("updated_at")
        private long updatedAt;
    }
}
