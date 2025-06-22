package org.qontak.viewcounter.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IncrementalViewUpdate {

    @JsonProperty("view_id")
    private String viewId;

    @JsonProperty("organization_id")
    private String organizationId;

    @JsonProperty("count")
    private int count;

    @Override
    public String toString() {
        return "IncrementalViewUpdate{" +
                "viewId='" + viewId + '\'' +
                ", organizationId='" + organizationId + '\'' +
                ", count=" + count +
                '}';
    }
}
