package org.qontak.roomstreamsanalytic.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomViewPlainFilter {

    private String id;
    private String name;

    @JsonProperty("organization_id")
    private String organizationId;

    private String filters;

    @JsonProperty("updated_at")
    private long updatedAt;

    @Override
    public String toString() {
        return "CustomViewPlainFilter{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", organizationId='" + organizationId + '\'' +
                ", filters='" + filters + '\'' +
                '}';
    }
}
