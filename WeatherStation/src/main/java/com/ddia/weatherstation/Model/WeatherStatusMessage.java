package com.ddia.weatherstation.Model;

import lombok.*;

@Data
@NoArgsConstructor
@Setter
@Getter
public class WeatherStatusMessage {
    private long station_id;
    private int s_no;
    private String battery_status;
    private long status_timestamp;
    private WeatherData weather;
}
