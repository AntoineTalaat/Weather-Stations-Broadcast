package com.ddia.weatherstation.Services;

import com.ddia.weatherstation.Model.WeatherData;
import com.ddia.weatherstation.Model.WeatherStatusMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Random;

@Component
public class WeatherStationMock {
    private static final int MESSAGE_FREQUENCY_MS = 1000;
    private static final int BATTERY_LOW_PERCENTAGE = 30;
    private static final int BATTERY_MEDIUM_PERCENTAGE = 40;
    private static final int BATTERY_HIGH_PERCENTAGE = 30;
    private static final int MESSAGE_DROP_RATE = 10;

    private static final Random random = new Random();

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MyHttpClient myHttpClient;
    private static long sNo = 1;
    private String[] cities = {"Alexandria", "Makkah", "Vancouver", "Zurich", "San Francisco",
    "Bergen", "Amsterdam", "Copenhagen", "Sydney", "Vienna"};
    private String[] latitude = {"31.20", "21.43", "49.25", "47.37", "37.77",
    "60.39", "52.37", "55.68", "-33.87", "48.21"};
    private String[] longitude = {"29.92", "39.83", "-123.12", "8.55", "-122.42",
    "5.32", "4.89", "12.57", "151.21", "16.37"};

    public WeatherStationMock(KafkaTemplate<String, String> kafkaTemplate, MyHttpClient myHttpClient) {
        this.kafkaTemplate = kafkaTemplate;
        this.myHttpClient = myHttpClient;
    }

    public void start(int cityId)
    {
        cityId--;
        if(cityId < 0 || cityId > 9)
            cityId = 0;
        String url = "https://api.open-meteo.com/v1/forecast?latitude="+latitude[cityId]+"&longitude="+longitude[cityId]+"&current_weather=true&hourly=temperature_2m,relativehumidity_2m,windspeed_10m&timeformat=unixtime";
        while (true) {
            WeatherStatusMessage statusMessage = generateStatusMessage(cityId, url);
            try {
                String json = toJson(statusMessage);
                boolean dropMessage = shouldDropMessage();
                if (!dropMessage) {
                    kafkaTemplate.send("weatherStation", json);
                }
            } catch (JsonProcessingException e) {
                System.err.println("Error generating JSON: " + e.getMessage());
            }

            try {
                Thread.sleep(MESSAGE_FREQUENCY_MS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    private WeatherStatusMessage generateStatusMessage(int cityId, String url) {
        long currentTime = System.currentTimeMillis() / 1000;
        int batteryStatusPercentage = generateBatteryStatusPercentage();
        String response = myHttpClient.getResponse(url);
        JSONObject jsonObject = new JSONObject(response);
//        System.out.println(jsonObject.getJSONObject("current_weather"));
        JSONObject currentWeather = jsonObject.getJSONObject("current_weather");
        JSONObject hourly = jsonObject.getJSONObject("hourly");
        WeatherStatusMessage statusMessage = new WeatherStatusMessage();
        statusMessage.setStation_id(cityId + 1);
        statusMessage.setS_no((int) sNo++);
        statusMessage.setBattery_status(generateBatteryStatus(batteryStatusPercentage));
        statusMessage.setStatus_timestamp(currentWeather.getLong("time"));
        WeatherData weatherData = new WeatherData();
        JSONArray humidityList = hourly.getJSONArray("relativehumidity_2m");
        double averageHumidity = 0;
        for(int i = 0; i < humidityList.length(); i++)
        {
            averageHumidity += (int) humidityList.get(i);
        }
        averageHumidity /=  humidityList.length();
        weatherData.setHumidity((int)averageHumidity);
//        weatherData.setHumidity((int)90);
        weatherData.setTemperature(currentWeather.getInt("temperature"));
        weatherData.setWind_speed(currentWeather.getDouble("windspeed"));
        statusMessage.setWeather(weatherData);
        return statusMessage;
    }
    private static int generateBatteryStatusPercentage() {
        int randomNumber = random.nextInt(100) + 1;
        if (randomNumber <= BATTERY_LOW_PERCENTAGE) {
            return randomNumber;
        } else if (randomNumber <= (BATTERY_LOW_PERCENTAGE + BATTERY_MEDIUM_PERCENTAGE)) {
            return randomNumber;
        } else {
            return randomNumber;
        }
    }

    private static String generateBatteryStatus(int batteryStatusPercentage) {
        if (batteryStatusPercentage <= BATTERY_LOW_PERCENTAGE) {
            return "low";
        } else if (batteryStatusPercentage <= (BATTERY_LOW_PERCENTAGE + BATTERY_MEDIUM_PERCENTAGE)) {
            return "medium";
        } else {
            return "high";
        }
    }

    private static boolean shouldDropMessage() {
        int randomNumber = random.nextInt(100) + 1;
        return randomNumber <= MESSAGE_DROP_RATE;
    }

    private static String toJson(WeatherStatusMessage statusMessage) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(statusMessage);
    }

}
