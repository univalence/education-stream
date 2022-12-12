package io.univalence.education_stream.kafka_streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class _02_reduce {

    private static final Logger logger = Logger.getLogger(_02_reduce.class.getName());

    public static Topology createTopology() {
        var builder = new StreamsBuilder();

        var input =
                builder.stream("input",
                        Consumed.with(Serdes.String(), Serdes.String()).withName("input-source"));

        var output =
                input
                        .flatMapValues(s -> {
                            try {
                                return List.of(Integer.valueOf(s));
                            } catch (NumberFormatException e) {
                                logger.warning("unable to convert " + s + " into integer");
                                // an empty does not send message
                                return List.of();
                            }
                        }, Named.as("to-int-converter"))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                        .reduce((value, sum) -> {
                                    // TODO Do the sum below
                                    var result = 0;
                                    logger.info("received value: " + value + " - current: " + sum + " -> result: " + result);
                                    return result;
                                },
                                Named.as("integer-sum-reducer"),
                                Materialized
                                        .<String, Integer, KeyValueStore<Bytes, byte[]>>as("sum-store")
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(Serdes.Integer()))
                        .toStream(Named.as("table-to-stream"));

        output
                .mapValues(Object::toString, Named.as("to-string-converter"))
                .to("output",
                        Produced.with(Serdes.String(), Serdes.String()).withName("output-sink"));

        return builder.build();
    }

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, _02_reduce.class.getName());

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
