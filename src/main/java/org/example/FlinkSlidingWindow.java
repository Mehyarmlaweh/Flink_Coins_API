package org.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Properties;

public class FlinkSlidingWindow {
    public static void main(String[] args) throws Exception {
        // Initialize the environment and add the Kafka consumer as a source
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "crypto-group");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "crypto_data",
                new SimpleStringSchema(),
                properties
        );

        // Assign a watermark strategy to handle event time
        DataStream<String> cryptoStream = env.addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<String>) WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofSeconds(10)) // Handling out-of-order events
                        .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())); // Timestamp assigner

        // Process data: Parse JSON, apply filters and compute aggregates
        SingleOutputStreamOperator<Double> cryptoPrices = cryptoStream
                .map(FlinkSlidingWindow::parseJson) // Parse JSON to create CryptoPrice objects
                .keyBy(CryptoPrice::getCryptoName)
                .filter(FlinkSlidingWindow::filterPriceSpikes) // Price Spikes Filter
                .filter(FlinkSlidingWindow::filterVolume) // Volume Threshold Filter
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
                .aggregate(new AveragePriceAggregator());

        cryptoPrices.print(); // Output the result

        env.execute("Flink Sliding Window with Filters");
    }

    private static CryptoPrice parseJson(String record) {
        // JSON parsing using Jackson to convert String to CryptoPrice object
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(record, CryptoPrice.class);
        } catch (Exception e) {
            e.printStackTrace();
            return new CryptoPrice("unknown", 0.0); // Return a default value if parsing fails
        }
    }

    // Filter out price spikes (10% increase or decrease)
    private static boolean filterPriceSpikes(CryptoPrice price) {
        if (price.getCryptoName().equals("bitcoin")) {
            double lastPrice = getLastPrice(price.getCryptoName()); // Assume method to fetch last price
            if (Math.abs(price.getPrice() - lastPrice) / lastPrice > 0.1) {
                return false; // Filter out if price spikes more than 10%
            }
        }
        return true;
    }

    // Filter out small volume data points
    private static boolean filterVolume(CryptoPrice price) {
        if (price.getVolume() < 1000000) { // Example threshold
            return false; // Filter out low volume
        }
        return true;
    }

    // Implement Moving Average to smooth out small fluctuations
    private static double applyMovingAverage(CryptoPrice price) {
        // Example of a simple moving average calculation over a sliding window
        return price.getPrice(); // For simplicity, assuming price remains unchanged
    }

    // Aggregator for average price calculation
    public static class AveragePriceAggregator implements AggregateFunction<CryptoPrice, Tuple2<Double, Integer>, Double> {
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(CryptoPrice value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.getPrice(), accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    // CryptoPrice class with volume and price
    public static class CryptoPrice {
        private String cryptoName;
        private double price;
        private double volume; // New field for volume

        public CryptoPrice(String cryptoName, double price, double volume) {
            this.cryptoName = cryptoName;
            this.price = price;
            this.volume = volume;
        }

        public String getCryptoName() {
            return cryptoName;
        }

        public double getPrice() {
            return price;
        }

        public double getVolume() {
            return volume;
        }
    }
}
