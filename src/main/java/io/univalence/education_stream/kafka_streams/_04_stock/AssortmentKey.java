package io.univalence.education_stream.kafka_streams._04_stock;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public record AssortmentKey(String banner, String productId) {
    // Hypermarket
    public static final String BANNER_HYP = "HYP";
    // Supermarket
    public static final String BANNER_SUP = "SUP";
    // Local shop (proximité)
    public static final String BANNER_PRX = "PRX";

    public static final Serde SERDE = new Serde();

    private static class Serde implements org.apache.kafka.common.serialization.Serde<AssortmentKey> {
        private static final Serializer<AssortmentKey> serializer =
                (String topic, AssortmentKey assortmentKey) ->
                        (assortmentKey.banner + "#" + assortmentKey.productId).getBytes(StandardCharsets.UTF_8);

        private static final Deserializer<AssortmentKey> deserializer =
                (String topic, byte[] data) -> {
                    var content = new String(data, StandardCharsets.UTF_8);
                    var fields = content.split("#", 2);
                    return new AssortmentKey(fields[0], fields[1]);
                };


        @Override
        public Serializer<AssortmentKey> serializer() {
            return null;
        }

        @Override
        public Deserializer<AssortmentKey> deserializer() {
            return null;
        }
    }

}
