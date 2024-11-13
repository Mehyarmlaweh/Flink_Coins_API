# Flink Kafka Crypto Price Monitoring Project

This project demonstrates the use of Apache Flink to ingest, process, and analyze cryptocurrency data from the CoinGecko API via Kafka. The project includes multiple filters (price spike detection, volume threshold, and moving average) and processes data in sliding windows for real-time analysis. The results are aggregated and visualized for better understanding.

## Key Features

- **Kafka Integration**: Data is ingested in real-time from the CoinGecko API and sent to Kafka.
- **Sliding Window**: Data is processed using a sliding window of 10 minutes with a slide interval of 1 minute.
- **Filters**:
    - **Price Spike Detection**: Filters out any price changes greater than 10% within the window.
    - **Volume Threshold**: Filters out low volume data points.
    - **Moving Average**: Applies a simple moving average to smooth out fluctuations in price data.
- **Aggregation**: Computes the average price for each cryptocurrency in each window.
- **Visualization**: Results are printed for real-time monitoring. (Optional: Use a tool like Grafana for visualization).

## Prerequisites

- Kafka running on `localhost:9092` (or adjust as necessary).
- Apache Flink set up and running.
- CoinGecko API key (if needed).

## Setup Instructions

1. **Install Kafka** (if not installed already):
    - Download and extract Kafka from [here](https://kafka.apache.org/downloads).
    - Start Zookeeper and Kafka brokers:
      ```bash
      bin/zookeeper-server-start.sh config/zookeeper.properties
      bin/kafka-server-start.sh config/server.properties
      ```

2. **Run the Crypto Data Producer**:
    - Compile and run the `CryptoDataProducer` class to stream data into Kafka from the CoinGecko API.

3. **Run the Flink Consumer**:
    - Compile and run the `FlinkSlidingWindowWithFilters` class to consume the data, apply filters, and perform aggregation.

4. **Visualize the Data**:
    - The aggregated data is printed to the console. Optionally, integrate with Grafana or any other visualization tool to display results in a dashboard.


## Example Output

### Flink Output:

Here is a sample output you may see printed on the console:

```bash
bitcoin: $90900 ethereum: $3302.5 
bitcoin: $90900 ethereum: $3302.5 
bitcoin: $90900 ethereum: $3302.5 
bitcoin: $90900 ethereum: $3302.5 
bitcoin: $90900 ethereum: $3302.5 
bitcoin: $90900 ethereum: $3302.5 
bitcoin: $90900 ethereum: $3302.5 
bitcoin: $90900 ethereum: $3302.5 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $90791 ethereum: $3278.49 
bitcoin: $91456 ethereum: $3302.5 
bitcoin: $91456 ethereum: $3302.5 
bitcoin: $91456 ethereum: $3302.5
...
```

### Kafka Output:
```
Processing crypto data...

-- [Price Spikes Filter] -- 
Bitcoin Price Spike Detected: Ignored price change of 12% over the last 10 minutes. 
Ethereum Price Spike Detected: Ignored price change of 15% over the last 10 minutes.

-- [Volume Threshold Filter] -- 
Bitcoin Volume Below Threshold: Ignored data with volume 750,000 (below 1,000,000). 
Ethereum Volume Below Threshold: Ignored data with volume 500,000 (below 1,000,000).

-- [Moving Average Filter] -- 
Bitcoin Moving Average (last 10 mins): $45,578.90 
Ethereum Moving Average (last 10 mins): $3,175.40

-- [Final Processed Data] -- 
New Data Processed - Average Price of Bitcoin: $45,578.90 
New Data Processed - Average Price of Ethereum: $3,175.40

Waiting for the next data batch...

```
## Running the Project

1. Start Kafka and Zookeeper.
2. Run the data producer to stream cryptocurrency data.
3. Run the Flink consumer job to process the data and apply filters.
4. (Optional) Set up visualization in Grafana for real-time monitoring.

## Conclusion

This project provides a comprehensive demonstration of real-time data processing using Apache Flink, with the added complexity of applying filters and aggregation. It can be expanded further by integrating additional data sources or performing more advanced analyses.

## Author : Mehyar MLAWEH
