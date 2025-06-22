package org.qontak.roomconsumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AggregatedViewCount {

    @JsonProperty("view_id")
    private String viewId;

    @JsonProperty("organization_id")
    private String organizationId;

    @JsonProperty("count")
    private Long count;

    @JsonProperty("version_updated")
    private long versionUpdated;

    @Override
    public String toString() {
        return "AggregatedViewCount{" +
                "viewId='" + viewId + '\'' +
                ", organizationId='" + organizationId + '\'' +
                ", count=" + count +
                ", versionUpdated=" + versionUpdated +
                '}';
    }
}
