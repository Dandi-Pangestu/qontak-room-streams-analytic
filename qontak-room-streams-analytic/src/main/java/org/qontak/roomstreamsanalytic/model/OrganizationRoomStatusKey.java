package org.qontak.roomstreamsanalytic.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrganizationRoomStatusKey {

    @JsonProperty("organization_id")
    private String organizationId;

    @JsonProperty("room_status")
    private String roomStatus;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrganizationRoomStatusKey that = (OrganizationRoomStatusKey) o;
        return Objects.equals(organizationId, that.organizationId) && Objects.equals(roomStatus, that.roomStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(organizationId, roomStatus);
    }
}
