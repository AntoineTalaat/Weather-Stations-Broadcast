package com.ddia.weatherstation.Services;

import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class Intializer implements CommandLineRunner {
    private final Environment environment;
    private final WeatherStationMock weatherStationMock;

    public Intializer(Environment environment, WeatherStationMock weatherStationMock) {
        this.environment = environment;
        this.weatherStationMock = weatherStationMock;
    }

    @Override
    public void run(String... args) throws Exception {
//        System.out.println("yeeeeeh" +environment.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        String city = environment.getProperty("city");
        int cityId;
        try
        {
            cityId = Integer.parseInt(city);
        }
        catch (Exception e)
        {
            cityId = 1;
        }
        System.out.println("Fetching weather information for city: " + cityId);
        this.weatherStationMock.start(cityId);
    }
}
