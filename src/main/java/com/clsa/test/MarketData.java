package com.clsa.test;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class MarketData {
    int id;

    String symbol;

    String bid;

    String ask;

    String last;

    @JsonProperty("last_updated_time")
    @JsonFormat(shape = JsonFormat.Shape.NUMBER, pattern = "s")
    Timestamp lastUpdatedTime;
}