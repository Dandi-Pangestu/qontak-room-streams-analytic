package org.qontak.roomstreamsanalytic.model;

import lombok.Data;

@Data
public class CustomViewFilter {

    private String field;
    private String operator;
    private String value;
}
