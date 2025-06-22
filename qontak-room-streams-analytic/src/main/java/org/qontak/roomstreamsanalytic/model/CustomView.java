package org.qontak.roomstreamsanalytic.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomView {

    private String id;
    private String name;

    @JsonProperty("organization_id")
    private String organizationId;

    private List<CustomViewFilter> filters;

    @Override
    public String toString() {
        return "CustomView{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", organizationId='" + organizationId + '\'' +
                ", filters=" + filters +
                '}';
    }
}
