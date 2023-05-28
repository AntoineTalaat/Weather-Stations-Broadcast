package com.ddia.weatherstation.Services;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class MyHttpClient {
    private RestTemplate restTemplate;

    public MyHttpClient() {
        this.restTemplate = new RestTemplate();
    }

    public String getResponse(String url) {
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        return response.getBody();
    }
}