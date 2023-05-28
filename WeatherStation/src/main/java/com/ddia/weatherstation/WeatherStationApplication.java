package com.ddia.weatherstation;

import com.ddia.weatherstation.Services.MyHttpClient;
import com.ddia.weatherstation.Services.WeatherStationMock;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.json.JSONObject;

import java.util.Scanner;

@SpringBootApplication
public class WeatherStationApplication implements ApplicationRunner {
    private final Environment environment;

    private final WeatherStationMock weatherStationMock;

    private final MyHttpClient myHttpClient;

    public WeatherStationApplication(Environment environment, WeatherStationMock weatherStationMock, MyHttpClient myHttpClient) {
        this.environment = environment;
        this.weatherStationMock = weatherStationMock;
        this.myHttpClient = myHttpClient;
    }


    public static void main(String[] args) {
        SpringApplication.run(WeatherStationApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
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


//        Scanner scanner = new Scanner(System.in);
//        int cityId = scanner.nextInt();
        System.out.println("Fetching weather information for city: " + cityId);
        // Add your weather data fetching logic here
//        this.weatherStationMock.start(cityId);
    }

}
