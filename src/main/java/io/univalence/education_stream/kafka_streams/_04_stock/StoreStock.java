package io.univalence.education_stream.kafka_streams._04_stock;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public record StoreStock(String storeId, String productId, Double quantity) {
    public static final Serde SERDE = new Serde();

    private static final Gson GSON = new Gson();

    public String toJson() {
        return GSON.toJson(this);
    }

    public static StoreStock fromJson(String json) {
        return GSON.fromJson(json, StoreStock.class);
    }

    private static class Serde implements org.apache.kafka.common.serialization.Serde<StoreStock> {
        private final Serializer<StoreStock> serializer =
                (String topic, StoreStock value) ->
                        value.toJson().getBytes(StandardCharsets.UTF_8);

        private final Deserializer<StoreStock> deserializer =
                (String topic, byte[] data) ->
                        StoreStock.fromJson(new String(data, StandardCharsets.UTF_8));

        @Override
        public Serializer<StoreStock> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<StoreStock> deserializer() {
            return deserializer;
        }

    }

}
