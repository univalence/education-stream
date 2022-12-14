package io.univalence.education.stream;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class StockProducer {

    public static void main(String[] args) throws Exception {

        //Define Schema for Stock in Avro
        //Parse CSV Line to GenericRecord
        //Send records to Kafka

        SchemaRegistryClient registryClient =
                new CachedSchemaRegistryClient("http://localhost:8081", 100);



        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);


        CSVParser.parse("stocks/APPL.csv", record -> {
            producer.send(new ProducerRecord<>("stocks", "APPL", record));
        });


        producer.close();



    }


    static String field(String name, String type) {
        return String.format("{\"name\": \"%s\", \"type\": \"%s\"}", name, type);
    }


    static final String schemaString = "{\"type\": \"record\", \"name\": \"myrecord\", \"fields\": [" +
            String.join(",",
                    field("date", "string"),
                    field("close", "double"),
                    field("volume", "long"),
                    field("open", "double"),
                    field("high", "double"),
                    field("low", "double")
            ) + "]}";


    //https://avro.apache.org/docs/1.10.2/spec.html#schema_primitive


    static final Schema schema = new Schema.Parser().parse(schemaString);


    //Date,Close,Volume,Open,High,Low

    static String usFormatToIso(String us) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        LocalDate date = LocalDate.parse(us, formatter);
        return date.format(DateTimeFormatter.ISO_LOCAL_DATE);
    }

    static Double toDollars(String d) {
        return Double.parseDouble(d.substring(1));
    }

    public static GenericRecord parseLine(String[] line, Schema schema) {
        var record = new GenericData.Record(schema);
        record.put(0, usFormatToIso(line[0]));
        record.put(1, toDollars(line[1]));
        record.put(2, Long.parseLong(line[2]));
        record.put(3, toDollars(line[3]));
        record.put(4, toDollars(line[4]));
        record.put(5, toDollars(line[5]));

        return record;
    }


    public static class CSVParser {

        public interface OnRecord {
            void run(GenericRecord record);
        }

        public static void parse(String filename, OnRecord onRecordRun) throws Exception {
            // Read the input CSV file
            try (InputStream resourceAsStream = CSVParser.class.getClassLoader().getResourceAsStream(filename);) {
                assert resourceAsStream != null;
                Reader ir = new InputStreamReader(resourceAsStream);

                CSVReader reader = new CSVReaderBuilder(ir).withSkipLines(1).build();


                // Parse and process the CSV data

                String[] line = null;

                while ((line = reader.readNext()) != null) {

                    onRecordRun.run(parseLine(line, schema));



                }
            } catch (IOException ignored) {
            }
        }
    }
}
