package org.qontak.viewcounter.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class CustomViewFilter {

    private String field;
    private String operator;
    private String value;

    @JsonProperty("value_array")
    private List<String> valueArray;

    @Override
    public String toString() {
        return "CustomViewFilter{" +
                "field='" + field + '\'' +
                ", operator='" + operator + '\'' +
                ", value='" + value + '\'' +
                ", valueArray=" + valueArray +
                '}';
    }
}
