package com.michalklempa.flink.solution.datastream;

import com.michalklempa.flink.exercise.datastream.PageViewEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

public class DataStream02FilterSolution {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.fromSource(JsonSourceFactory.create(), WatermarkStrategy.noWatermarks(), "Json Source");

        /**
         * Solution with .filter
         */
        inputStream
                .map(new RichMapFunction<String, PageViewEvent>() {
                    private transient ObjectMapper notSerializedObjectMapper;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.notSerializedObjectMapper = new ObjectMapper();
                    }

                    @Override
                    public PageViewEvent map(String value) throws Exception {
                        return notSerializedObjectMapper.readValue(value, PageViewEvent.class);
                    }
                })
                .filter(event -> "404".equals(event.page))
                .print();

        /**
         * Solution with eager validation and returning null
         * requiring null-check in latter pipeline stages.
         *
         * Provided as example how not to approach data problems - null values cause bugs.
         */
        inputStream
                .map(new RichMapFunction<String, PageViewEvent>() {
                    private transient ObjectMapper notSerializedObjectMapper;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.notSerializedObjectMapper = new ObjectMapper();
                    }

                    @Override
                    public PageViewEvent map(String value) throws Exception {
                        PageViewEvent event = notSerializedObjectMapper.readValue(value, PageViewEvent.class);
                        if ("404".equals(event.page)) {
                            return event;
                        }
                        return null;
                    }
                })
                .filter(x -> x != null) // Example filter, in case of any other operator, null-check is required
                // .map(x -> x != null ? x.toString : "null")
                .print();

        /**
         * Solution using map and List as result - to avoid nulls
         *
         * Demonstration on how .flatMap actually works internally.
         * This is on the way towards explaining the flatMap itself - map returing DataStream of Lists > flatten immediately.
         *
         * Implementation of .flatten itself is suggested exercise for the reader.
         */
        inputStream
                .map(new RichMapFunction<String, List<PageViewEvent>>() {
                    private transient ObjectMapper notSerializedObjectMapper;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.notSerializedObjectMapper = new ObjectMapper();
                    }

                    @Override
                    public List<PageViewEvent> map(String value) throws Exception {
                        PageViewEvent event = notSerializedObjectMapper.readValue(value, PageViewEvent.class);
                        if ("404".equals(event.page)) {
                            return Collections.singletonList(event);
                        }
                        return Collections.emptyList();
                    }
                })
                .filter(x -> x.size() > 0) // Example filter, null check not required
                .map(x -> x.get(0)) // real operation can rely on non-null value, un-nest the list
                .print();

        /**
         * Solution using flatMap on eager deserialization
         */
        inputStream
                .flatMap(new RichFlatMapFunction<String, PageViewEvent>() {
                    private transient ObjectMapper notSerializedObjectMapper;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.notSerializedObjectMapper = new ObjectMapper();
                    }

                    @Override
                    public void flatMap(String value, Collector<PageViewEvent> out) throws Exception {
                        PageViewEvent event = notSerializedObjectMapper.readValue(value, PageViewEvent.class);
                        if ("404".equals(event.page)) {
                            out.collect(event);
                        }
                        /**
                         * Not satisfying the criteria - not collecting > no output.
                         */
                    }
                })
                .print();

        env.execute(DataStream02FilterSolution.class.getName());
    }
}
;
