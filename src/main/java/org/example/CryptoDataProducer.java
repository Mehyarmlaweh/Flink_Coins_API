package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.Scanner;

    public class CryptoDataProducer {
    private static final String TOPIC = "crypto_data";
    private static final String[] CRYPTOS = {"bitcoin", "ethereum"};
    private static final String API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd";
    private static final String API_KEY = "CG-vLCaD4o99n6dDLcijnfDfKK1";

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Fetch data and send to Kafka in a loop
        while (true) {
            String cryptoData = fetchCryptoData();
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "crypto_prices", cryptoData);
            producer.send(record);

            System.out.println("Sent data: " + cryptoData);
            Thread.sleep(60000); // Wait 1 minute before the next API call
        }
    }

    // Method to fetch cryptocurrency data from CoinGecko API
    private static String fetchCryptoData() throws IOException {
        URL url = new URL(API_URL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // Add the API key to the request header
        conn.setRequestProperty("Authorization", "Bearer " + API_KEY);

        Scanner scanner = new Scanner(conn.getInputStream());
        StringBuilder response = new StringBuilder();
        while (scanner.hasNext()) {
            response.append(scanner.nextLine());
        }
        scanner.close();

        // Parse and format the JSON response
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(response.toString());
        StringBuilder formattedData = new StringBuilder();

        for (String crypto : CRYPTOS) {
            JsonNode cryptoNode = rootNode.path(crypto).path("usd");
            formattedData.append(crypto).append(": $").append(cryptoNode.asText()).append(" ");
        }

        return formattedData.toString();
    }
}
