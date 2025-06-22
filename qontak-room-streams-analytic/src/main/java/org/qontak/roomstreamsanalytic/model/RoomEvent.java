package org.qontak.roomstreamsanalytic.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RoomEvent {

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

    @JsonProperty("division_id")
    private String divisionId;

    @JsonProperty("is_blocked")
    private Boolean isBlocked;

    @JsonProperty("updated_at")
    private long updatedAt;

    @Override
    public String toString() {
        return "RoomEvent{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", status='" + status + '\'' +
                ", type='" + type + '\'' +
                ", channelIntegrationId='" + channelIntegrationId + '\'' +
                ", accountUniqId='" + accountUniqId + '\'' +
                ", organizationId='" + organizationId + '\'' +
                ", divisionId='" + divisionId + '\'' +
                ", isBlocked=" + isBlocked +
                ", updatedAt=" + updatedAt +
                '}';
    }
}
