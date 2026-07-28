package com.michalklempa.flink.exercise.wordcount;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

public class SentenceSourceFactory {
    public static DataGeneratorSource<String> create() {
        String[] sentences = {
                "the quick brown fox jumps over the lazy dog",
                "apache flink is a distributed stream processing framework",
                "flink provides high throughput and low latency data processing",
                "the flink community is very active and helpful",
                "stream processing enables real time analytics",
                "windows and watermarks are important flink concepts",
                "event time processing is more accurate than processing time",
                "flink can process unbounded data streams efficiently"
        };

        Random random = new Random(42L);
        GeneratorFunction<Long, String> generator = index ->
                sentences[(int) (index % sentences.length)];

        return new DataGeneratorSource<>(
                generator,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(5),
                TypeInformation.of(String.class));
    }
}
