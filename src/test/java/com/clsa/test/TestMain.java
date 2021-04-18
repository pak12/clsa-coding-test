package com.clsa.test;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class TestMain {
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws IOException, InterruptedException {
        MarketDataProcessor marketDataProcessor = new MarketDataProcessor();

        ObjectMapper objectMapper = new ObjectMapper();

        // Simulate high speed market data update on single thread
        String line;
        Random r = new Random();
        while (scanner.hasNextLine()) {
            line = scanner.nextLine();

            // Pause randomly
            if (r.nextInt(10) == 9) {
                Thread.sleep(100);
            }
            try {
                MarketData marketData = objectMapper.readValue(line, MarketData.class);
                marketDataProcessor.onMessage(marketData);
            } catch (JsonMappingException e) {
                e.printStackTrace();
            }
        }
        marketDataProcessor.end();
    }
}
