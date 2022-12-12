package io.univalence.education_stream.kafka_streams._04_stock;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class StockProcessorMain {

    public static final String STORE_STOCK_TOPIC = "store-stock";
    public static final String LOCATION_TOPIC = "location";
    public static final String ASSORTMENT_TOPIC = "assortment";

    private static final Logger logger = Logger.getLogger(StockProcessorMain.class.getName());

    public static Topology createTopology() {
        var builder = new StreamsBuilder();

        var stocks =
                builder.stream(STORE_STOCK_TOPIC, Consumed.with(StockKey.SERDE, StoreStock.SERDE)
                        .withName("store-stock-source"));

        var locations =
                builder.table(LOCATION_TOPIC, Consumed.with(Serdes.String(), Location.SERDE)
                                .withName("location-source"),
                        Materialized
                                .<String, Location, KeyValueStore<Bytes, byte[]>>as("location-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Location.SERDE)
                );

        var assortments =
                builder.table(ASSORTMENT_TOPIC, Consumed.with(AssortmentKey.SERDE, Assortment.SERDE)
                                .withName("assortment-source"),
                        Materialized
                                .<AssortmentKey, Assortment, KeyValueStore<Bytes, byte[]>>as("assortment-store")
                                .withKeySerde(AssortmentKey.SERDE)
                                .withValueSerde(Assortment.SERDE)
                );

        // TODO complete the topology, so we compute the stock at e-commerce assortment

        return builder.build();
    }

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, StockProcessorMain.class.getName());

        logger.info("Create topology");
        var topology = createTopology();
        logger.info(topology.describe().toString());

        try (var streamService = new KafkaStreams(topology, properties)) {
            var latch = new CountDownLatch(1);
            streamService.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streamService::close));
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
