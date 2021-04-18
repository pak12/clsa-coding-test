package com.clsa.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MarketDataProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MarketDataProcessor.class);

    private static final int THROTTLE_PER_SEC = 100;

    // Store the latest data of the symbol
    private final Map<String, MarketData> symbolMarketDataMap = new ConcurrentHashMap<>();

    // Map of symbols and their last published time
    private final HashMap<String, Instant> symbolLastPublishedInstantMap = new HashMap<>();

    private final BlockingQueue<String> symbolPublishQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean run = new AtomicBoolean(true);
    private Instant throttleStartInstant = null;
    private int throttle;

    public MarketDataProcessor() {
        // This is the thread that consumes symbolPublishQueue and decides whether or not to publish the market data
        Thread publisher = new Thread(new MarketDataPublisher());
        publisher.start();
    }

    // Receive incoming market data
    // Expects to be called very frequently
    // Make this function as light as possible to prevent blocking
    public void onMessage(MarketData data) {
        // Store the data
        String symbol = data.getSymbol();
        symbolMarketDataMap.put(symbol, data);

        // Check if this symbol already in the queue to be processed, skip if true to prevent delaying the process of other symbols
        if (!symbolPublishQueue.contains(symbol)) {
            logger.debug("Pushed [{}], id [{}]", symbol, data.getId());
            symbolPublishQueue.add(symbol);
        } else {
            logger.debug("Skipped [{}], id [{}]", symbol, data.getId());
        }
    }

    public void end() {
        logger.info("Ending...");
        run.lazySet(false);
    }

    // Publish aggregated and throttled market data
    public void publishAggregatedMarketData(MarketData data) {
        // For test
        logger.debug("publishAggregatedMarketData: {}", data);

        // Do Nothing, assume implemented.
    }

    class MarketDataPublisher implements Runnable {
        @Override
        public void run() {
            while (run.get()) {
                try {
                    String symbol = symbolPublishQueue.poll(1, TimeUnit.SECONDS);
                    if (symbol == null) continue;

                    // To discuss: If no throttle is found
                    // should we keep taking from the queue or wait for throttle to reset?
                    if (!acquireThrottle()) continue;

                    if (!shouldPublishSymbol(symbol)) continue;

                    // Publish latest data
                    publishAggregatedMarketData(symbolMarketDataMap.get(symbol));

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("Publisher end");
        }

        private boolean shouldPublishSymbol(String symbol) {
            Instant now = Instant.now();
            Instant lastPublishedInstant = symbolLastPublishedInstantMap.get(symbol);
            if (lastPublishedInstant == null) {
                symbolLastPublishedInstantMap.put(symbol, now);
                return true;
            }

            Duration duration = Duration.between(lastPublishedInstant, now);

            // Passed 1s, reset map
            if (duration.compareTo(Duration.ofSeconds(1)) > 0) {
                symbolLastPublishedInstantMap.put(symbol, now);
                return true;
            }

            logger.debug("shouldPublishSymbol: Skipped symbol [{}]", symbol);
            return false;
        }

        private boolean acquireThrottle() {
            String throttleCountLogText = "Throttle count = [{}]";
            Instant now = Instant.now();
            if (throttleStartInstant == null) {
                throttleStartInstant = now;
                throttle = THROTTLE_PER_SEC - 1;
                logger.debug(throttleCountLogText, throttle);
                return true;
            }

            Duration duration = Duration.between(throttleStartInstant, now);

            // Passed 1s, reset throttle
            if (duration.compareTo(Duration.ofSeconds(1)) > 0) {
                logger.debug("Throttle reset");
                throttleStartInstant = now;
                throttle = THROTTLE_PER_SEC - 1;
                logger.debug(throttleCountLogText, throttle);
                return true;
            } else {
                if (throttle > 0) {
                    throttle--;
                    logger.debug(throttleCountLogText, throttle);
                    return true;
                }
                logger.debug("Throttle used up");
                return false;
            }
        }

    }

}