package io.univalence.education_stream.kafka_streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.List;
import java.util.Map;

public class _03_join_generator {

    public record Player(String id, String name) {}
    public record Score(String id, Integer value) {}

    public static final List<Player> PLAYERS =
            List.of(
                    new Player("1", "jon"),
                    new Player("2", "mary"),
                    new Player("3", "tom"),
                    new Player("4", "sophie"),
                    new Player("5", "marc"),
                    new Player("6", "marc")
            );

    public static final List<Score> SCORES =
            List.of(
                    new Score("1", 0),
                    new Score("2", 0),
                    new Score("3", 0),
                    new Score("4", 0),
                    new Score("5", 0),
                    new Score("6", 0),
                    new Score("1", 10),
                    new Score("4", 8),
                    new Score("1", 20),
                    new Score("4", 12),
                    new Score("5", 5),
                    new Score("1", 22),
                    new Score("4", 10),
                    new Score("5", 8),
                    new Score("2", 28),
                    new Score("4", 14),
                    new Score("5", 16),
                    new Score("2", 32)
            );

    public static void main(String[] args) throws Exception {
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(
                             Map.of(
                                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                     "localhost:9092"
                             ),
                             Serdes.String().serializer(),
                             Serdes.String().serializer()
                     )) {


            for (var player : PLAYERS) {
                var record = new ProducerRecord<>("players", player.id, player.name);
                producer.send(record).get();
                System.out.println("player sent: " + player);
                Thread.sleep(500);
            }

            for (var score: SCORES) {
                var record = new ProducerRecord<>("scores", score.id, score.value.toString());
                producer.send(record);
                System.out.println("score sent: " + score);
                Thread.sleep(1000);
            }
        }
    }

}
