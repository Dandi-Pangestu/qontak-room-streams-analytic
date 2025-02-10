package org.qontak.roomconsumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RoomStatusCount {

    @JsonProperty("organization_id")
    private String organizationId;

    private String status;
    private Long value;

    @Override
    public String toString() {
        return "RoomStatusCount{" +
                "organizationId='" + organizationId + '\'' +
                ", status='" + status + '\'' +
                ", value=" + value +
                '}';
    }
}
