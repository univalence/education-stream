package io.univalence.education_stream.kafka_streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class _03_join {

    private static final Logger logger = Logger.getLogger(_03_join.class.getName());

    public static Topology createTopology() {
        var builder = new StreamsBuilder();

        // TODO Modify the topology below so that scores add up for each key
        // eg. The messages below
        //   [key=1, value=10], [key=2, value=2], [key=1, value=20]
        // are converted into
        //   [key=2, value=2], [key=1, value=30]

        var players =
                builder.table("players",
                        Consumed.with(Serdes.String(), Serdes.String()).withName("players-source"),
                        Materialized
                                .<String, String, KeyValueStore<Bytes, byte[]>>as("players-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()));

        var scores =
                builder.stream("scores",
                                Consumed.with(Serdes.String(), Serdes.String()).withName("scores-source"))
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
                        .reduce((oldScore, newScore) -> newScore,
                                Named.as("last-score-reducer"),
                                Materialized
                                        .<String, Integer, KeyValueStore<Bytes, byte[]>>as("scores-store")
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(Serdes.Integer())
                        );

        var playerScores =
                players.leftJoin(scores, (name, score) -> {
                            if (score == null) {
                                return name + " => 0";
                            } else {
                                return name + " => " + score;
                            }
                        },
                        Named.as("player-score-left-join"));

        playerScores
                .toStream(Named.as("player-scores-table-to-stream"))
                .to("player-scores",
                        Produced.with(Serdes.String(), Serdes.String()).withName("player-scores-sink"));

        return builder.build();
    }

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, _03_join.class.getName());

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
