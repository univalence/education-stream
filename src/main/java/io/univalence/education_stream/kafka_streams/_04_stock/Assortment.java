package io.univalence.education_stream.kafka_streams._04_stock;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

public record Assortment(String banner, String productId, LocalDate start, LocalDate end) {
    public static final Serde SERDE = new Serde();

    private static final Gson GSON =
            (new GsonBuilder())
                    .registerTypeAdapter(LocalDate.class, new LocalDateTypeAdapter())
                    .create();


    public String toJson() {
        return GSON.toJson(this);
    }

    public static Assortment fromJson(String json) {
        return GSON.fromJson(json, Assortment.class);
    }

    public static class Serde implements org.apache.kafka.common.serialization.Serde<Assortment> {
        private final Serializer<Assortment> serializer =
                (String topic, Assortment assortment) ->
                        assortment.toJson().getBytes(StandardCharsets.UTF_8);

        private final Deserializer<Assortment> deserializer =
                (String topic, byte[] data) ->
                        Assortment.fromJson(new String(data, StandardCharsets.UTF_8));

        @Override
        public Serializer<Assortment> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<Assortment> deserializer() {
            return deserializer;
        }
    }

}
