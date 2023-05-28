package com.example.centralstationkafka.bitcaskAndParquet.BaseCentralStation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class StationMessageGenerator {
    public static void main(String[] args) {
        List<StationMessage> stationMessageList = generateRandomStationMessages(10005);
        ParquetFileWriter writer = new ParquetFileWriter();

        for (StationMessage message : stationMessageList) {
            writer.receive(message);
        }


    }

    private static List<StationMessage> generateRandomStationMessages(int count) {
        List<StationMessage> stationMessageList = new ArrayList<>();
        Random random = new Random();

        long currentTimeMillis = System.currentTimeMillis();
        long threeDaysInMillis = TimeUnit.DAYS.toMillis(3);

        for (int i = 0; i < count; i++) {
            StationMessage stationMessage = new StationMessage();
            stationMessage.station_id = random.nextBoolean() ? 7 : 5;
            stationMessage.s_no = i + 1;
            stationMessage.status_timestamp = currentTimeMillis - random.nextInt((int) threeDaysInMillis);
            stationMessage.battery_status = getRandomBatteryStatus();
            stationMessage.weather = new StationMessage.Weather();
            stationMessage.weather.humidity = random.nextInt(100);
            stationMessage.weather.temperature = random.nextInt(50);
            stationMessage.weather.wind_speed = random.nextInt(50);
            stationMessageList.add(stationMessage);
        }

        return stationMessageList;
    }

    private static String getRandomBatteryStatus() {
        String[] batteryStatuses = {"LOW", "MEDIUM", "HIGH"};
        Random random = new Random();
        int index = random.nextInt(batteryStatuses.length);
        return batteryStatuses[index];
    }

}
