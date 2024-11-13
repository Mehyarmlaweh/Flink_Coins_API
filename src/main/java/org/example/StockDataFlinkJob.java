package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.state.ValueState;
import org.apache.flink.streaming.api.state.ValueStateDescriptor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class StockDataFlinkJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka properties setup
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker address
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "stock-group");

        // Set up Kafka consumer to listen to stock data topic (e.g., "stock-price")
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "stock-price",  // Kafka topic name
                new StringDeserializer(), // Deserializer
                properties);

        // Create a stream from Kafka
        DataStream<String> stockStream = env.addSource(kafkaConsumer);

        // Process the stream by applying a sliding window
        DataStream<String> notificationsStream = stockStream
                .map(new RichMapFunction<String, String>() {
                    private transient ValueState<Double> lastPriceState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // Initialize the state to store the last price
                        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                                "lastPrice",  // State name
                                TypeInformation.of(Double.class), // State type
                                0.0); // Default value
                        lastPriceState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        // Assuming the stock data is in the format "symbol:price"
                        String[] parts = value.split(":");
                        String symbol = parts[0];
                        Double price = Double.parseDouble(parts[1]);

                        // Retrieve the last recorded price for Bitcoin
                        Double lastPrice = lastPriceState.value();

                        // Notify if the price change exceeds $50
                        if ("bitcoin".equals(symbol) && Math.abs(price - lastPrice) >= 50) {
                            // Create the notification message
                            String notification = "Bitcoin price changed by " + (price - lastPrice) +
                                    " USD. New Price: " + price;
                            System.out.println(notification);  // Print or trigger an external notification

                            // Update the last seen price
                            lastPriceState.update(price);
                            return notification;
                        }

                        // No change or no notification required
                        return "No significant change in Bitcoin price.";
                    }
                });

        // Print the notifications stream to stdout
        notificationsStream.print();

        // Execute the Flink job
        env.execute("Bitcoin Price Change Notification Job");
    }
}
