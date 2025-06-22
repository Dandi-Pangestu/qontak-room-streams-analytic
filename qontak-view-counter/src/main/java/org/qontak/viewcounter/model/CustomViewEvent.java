package org.qontak.viewcounter.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomViewEvent {

    private String id;
    private String name;

    @JsonProperty("organization_id")
    private String organizationId;

    private String filters;

    @JsonProperty("created_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime createdAt;

    @JsonProperty("updated_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime updatedAt;

    @JsonProperty("version_updated")
    private long versionUpdated;

    public boolean matches(RoomEvent event) {
        List<CustomViewFilter> filterList = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create()
                .fromJson(filters, new TypeToken<List<CustomViewFilter>>() {}.getType());

        for (CustomViewFilter filter : filterList) {
            String field = filter.getField();
            String operator = filter.getOperator();

            String eventValue = null;
            try {
                eventValue = getEventFieldValue(event, field);
            } catch (Exception e) {
                continue;
            }

            switch (operator) {
                case "is": {
                    String value = filter.getValue();
                    if (!Objects.equals(eventValue, value)) return false;
                    break;
                }
                case "is_not": {
                    String value = filter.getValue();
                    if (Objects.equals(eventValue, value)) return false;
                    break;
                }
                case "in": {
                    List<String> value = filter.getValueArray();
                    if (value != null && !value.isEmpty()) {
                        if (!value.contains(eventValue)) return false;
                        break;
                    }
                }
                default:
                    return false;
            }
        }

        return true;
    }

    private String getEventFieldValue(RoomEvent event, String field) throws Exception {
        switch (field) {
            case "status":
                return event.getStatus();
            case "division_id":
                return event.getDivisionId();
            default:
                throw new Exception("There is not field macthed");
        }
    }

    @Override
    public String toString() {
        return "CustomViewEvent{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", organizationId='" + organizationId + '\'' +
                ", filters=" + filters +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                ", versionUpdated=" + versionUpdated +
                '}';
    }
}
