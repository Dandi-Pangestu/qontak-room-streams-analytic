package org.qontak.roomstreamsanalytic.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RoomStatusEvent {

    private String id;
    private String name;
    private String status;

    @JsonProperty("organization_id")
    private String organizationId;

    @Override
    public String toString() {
        return "RoomStatusEvent{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", status='" + status + '\'' +
                ", organizationId='" + organizationId + '\'' +
                '}';
    }
}
