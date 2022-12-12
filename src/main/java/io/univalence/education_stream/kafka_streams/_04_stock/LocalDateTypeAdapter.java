package io.univalence.education_stream.kafka_streams._04_stock;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LocalDateTypeAdapter extends TypeAdapter<LocalDate> {
    @Override
    public void write(JsonWriter out, LocalDate value) throws IOException {
        out.jsonValue(value.format(DateTimeFormatter.ISO_DATE));
    }

    @Override
    public LocalDate read(JsonReader in) throws IOException {
        var data = in.nextString();
        if (data == null) {
            return null;
        } else {
            return LocalDate.parse(data, DateTimeFormatter.ISO_DATE);
        }
    }
}
