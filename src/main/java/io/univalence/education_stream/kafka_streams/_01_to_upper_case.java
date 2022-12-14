package io.univalence.education_stream.kafka_streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class _01_to_upper_case {

    private static final Logger logger = Logger.getLogger(_01_to_upper_case.class.getName());

    public static Topology createTopology() {
        var builder = new StreamsBuilder();

        var input =
                builder.stream("input",
                        Consumed.with(Serdes.String(), Serdes.String()).withName("input-source"));

        // TODO Change the code below, so text appears in uppercase
        var output =
                input.mapValues(
                        s -> {
                            var inputValue = s;
                            var outputValue = inputValue;

                            logger.info("Convert "  + inputValue + " --> " + outputValue);

                            return outputValue;
                        },
                        Named.as("convert-to-uppercase")
                );

        output.to("output",
                Produced.with(Serdes.String(), Serdes.String()).withName("output-sink"));

        return builder.build();
    }

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, _01_to_upper_case.class.getName());

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
