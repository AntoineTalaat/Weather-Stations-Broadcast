package com.example.centralstationkafka.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@Setter
@Getter
@NoArgsConstructor
public class RainDetectionMessage {
    private String key;
    private long station_no;
    private String warning;
    private String allWeatherMessage;
}
