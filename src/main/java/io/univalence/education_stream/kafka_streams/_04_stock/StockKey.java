package io.univalence.education_stream.kafka_streams._04_stock;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public record StockKey(String storeId, String productId) {

    public static final Serde SERDE = new Serde();

    private static class Serde implements org.apache.kafka.common.serialization.Serde<StockKey> {
        private static final Serializer<StockKey> serializer =
                (String topic, StockKey stockKey) ->
                        (stockKey.storeId + "#" + stockKey.productId).getBytes(StandardCharsets.UTF_8);

        private static final Deserializer<StockKey> deserializer =
                (String topic, byte[] data) -> {
                    var content = new String(data, StandardCharsets.UTF_8);
                    var fields = content.split("#", 2);
                    return new StockKey(fields[0], fields[1]);
                };

        @Override
        public Serializer<StockKey> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<StockKey> deserializer() {
            return deserializer;
        }
    }
}
