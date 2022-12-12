package io.univalence.education_stream.kafka_streams._04_stock;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.List;

public record Location(String storeId, String banner, List<String> serviceIds) {

    public static final Serde SERDE = new Serde();

    private static final Gson GSON = new Gson();

    public String toJson() {
        return GSON.toJson(this);
    }

    public static Location fromJson(String json) {
        return GSON.fromJson(json, Location.class);
    }

    private static class Serde implements org.apache.kafka.common.serialization.Serde<Location> {
        private final Serializer<Location> serializer =
                (String topic, Location location) ->
                        location.toJson().getBytes(StandardCharsets.UTF_8);

        private final Deserializer<Location> deserializer =
                (String topic, byte[] data) ->
                        Location.fromJson(new String(data, StandardCharsets.UTF_8));

        @Override
        public Serializer<Location> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<Location> deserializer() {
            return deserializer;
        }
    }

}
