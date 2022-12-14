package io.univalence.education;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaTools {


    static void displayTopic(String name) {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG,"dump-" + UUID.randomUUID().toString());


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Pattern.compile(name));

            System.out.print("\033[H\033[2J");
            System.out.flush();


            ConsumerRecords<String, String> records = consumer.poll(0);
            
            do {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("p: %d, o: %d, v: %s\n", record.partition(), record.offset(), record.value());
                }


                records = consumer.poll(1000);
            } while (!records.isEmpty());
        }


    }

    public static void main(String[] args) throws InterruptedException {

       while (true) {
           displayTopic("input");
            Thread.sleep(10000);
       }





    }
}
