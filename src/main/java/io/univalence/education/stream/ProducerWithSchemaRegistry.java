package io.univalence.education.stream;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class ProducerWithSchemaRegistry {

    public static void main(String[] args) throws RestClientException, IOException {
        SchemaRegistryClient registryClient =
                new CachedSchemaRegistryClient("http://localhost:8081", 100);



        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

        String schemaString = "{\"type\": \"record\", \"name\": \"myrecord\", \"fields\": [{\"name\": \"field1\", \"type\": \"string\"}]}";

        Schema schema = new Schema.Parser().parse(schemaString);
        GenericRecord record = new GenericData.Record(schema);
        record.put("field1", "value1");

        producer.send(new ProducerRecord<>("myrecord", null, record));

        //schemaRegistryClient.updateCompatibility("input-value", "FULL");

        producer.close();


    }

}
