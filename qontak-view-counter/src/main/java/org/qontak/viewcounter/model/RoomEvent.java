package org.qontak.viewcounter.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

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

    @JsonProperty("created_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime createdAt;

    @JsonProperty("updated_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime updatedAt;

    @JsonProperty("version_updated")
    private long versionUpdated;

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
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                ", versionUpdated=" + versionUpdated +
                '}';
    }
}
