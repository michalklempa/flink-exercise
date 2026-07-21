package com.michalklempa.flink.exercise.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class JsonSourceFactory {
    public static class DeterministicClickStream {
        String[] pages = {"home", "cart", "checkout", "detail", "registration", "login", "404"};

        public ClickEvent next(long index) {
            int pages_index = (int) (index % ((long) pages.length));
            long user_id = index % 100L;
            long client_time = 1784541600000L + index * 7L;

            return new ClickEvent(
                    client_time, pages[pages_index], user_id
            );
        }
    }

    public static DataGeneratorSource<String> create() {
        final DeterministicClickStream deterministicClickStream = new DeterministicClickStream();
        GeneratorFunction<Long, String> generator = index -> deterministicClickStream.next(index).toString();

        return new DataGeneratorSource<>(
                generator,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(60),
                TypeInformation.of(String.class));
    }
}
