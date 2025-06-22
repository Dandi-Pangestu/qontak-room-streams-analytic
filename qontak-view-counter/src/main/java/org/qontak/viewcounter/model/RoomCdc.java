package org.qontak.viewcounter.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RoomCdc {

    private Before before;
    private After after;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Before {

        private String id;
        private String status;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class After {

        private String id;
        private String name;
        private String status;
        private String type;

        @JsonProperty("channel_integration_id")
        private String channelIntegrationId;

        @JsonProperty("account_uniq_id")
        private String accountUniqId;

        @JsonProperty("organization_id")
        private String organizationId;

        @JsonProperty("created_at")
        private long createdAt;

        @JsonProperty("updated_at")
        private long updatedAt;

        @JsonProperty("division_id")
        private String divisionId;

        @JsonProperty("is_blocked")
        private Boolean isBlocked;
    }

    @Override
    public String toString() {
        return "RoomCDC{" +
                "before=" + before +
                ", after=" + after +
                '}';
    }
}
