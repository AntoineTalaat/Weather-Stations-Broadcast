package com.ddia.weatherstation.Model;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class WeatherData {
    private int humidity;
    private int temperature;
    private double wind_speed;
}
