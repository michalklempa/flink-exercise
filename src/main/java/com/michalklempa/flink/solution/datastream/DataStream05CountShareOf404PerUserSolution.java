package com.michalklempa.flink.solution.datastream;

import com.michalklempa.flink.exercise.datastream.PageViewEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class DataStream05CountShareOf404PerUserSolution {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.fromSource(JsonSourceFactory.create(), WatermarkStrategy.noWatermarks(), "Json Source");

        /**
         * Unlike in the previous exercises we must not filter out anything.
         * The share of 404s is "404 page views / all page views", so the denominator
         * needs every single event.
         */
        DataStream<PageViewEvent> pageViewStream = inputStream
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
                });

        DataStream<PageViewEvent> watermarkedStream = pageViewStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<PageViewEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PageViewEvent>() {
                            @Override
                            public long extractTimestamp(PageViewEvent element, long recordTimestamp) {
                                return element.client_time;
                            }
                        })
        );

        /**
         * Solution of "share of 404s over all page views per user per hour" using keyBy, window and ProcessWindowFunction
         *
         * Notice the elements of the window must be kept around in memory until the window trigger
         * fires. Even though we only need two counters. With 100 users and around 85 page views
         * per user per hour, it is not much, but it grows with every event.
         *
         * Window [2026-07-20T10:00:00Z, 2026-07-20T11:00:00Z) User Id: 42 404s share: 0.1411764705882353
         */
        watermarkedStream
                .keyBy(new KeySelector<PageViewEvent, Long>() {
                    @Override
                    public Long getKey(PageViewEvent value) throws Exception {
                        return value.user_id;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessWindowFunction<PageViewEvent, String, Long, TimeWindow>() {
                    @Override
                    public void process(Long user_id, ProcessWindowFunction<PageViewEvent, String, Long, TimeWindow>.Context context, Iterable<PageViewEvent> elements, Collector<String> out) throws Exception {
                        long count404 = 0;
                        long countAll = 0;
                        for (PageViewEvent element : elements) {
                            countAll++;
                            if ("404".equals(element.page)) {
                                count404++;
                            }
                        }
                        out.collect("Window [" +
                                Instant.ofEpochMilli(context.window().getStart()) +
                                ", " +
                                Instant.ofEpochMilli(context.window().getEnd()) +
                                ") User Id: " +
                                user_id +
                                " 404s share: " +
                                (double) count404 / countAll);
                    }
                });
//                .print();

        /**
         * Solution of "share of 404s over all page views per user per hour" using keyBy, window and aggregate
         * in event time.
         *
         * The accumulator is a Tuple2<Long, Long>: f0 is the count of 404 page views, f1 is the count of all
         * page views. Both counts are collected in a single pass, so the window keeps just two numbers
         * per user and window, not the elements.
         *
         * Share is computed in getResult, the accumulator stays a plain pair of counters, which
         * is the only form that can be merged (a share of shares is not a share).
         *
         * We must use TumblingEventTimeWindows, this time with one hour size.
         *
         * Notice that data timestamps advance by 421 ms per event, one hour of event time is therefore
         * around 8500 events. With 100 events per second rate limit, we have to wait about 90 seconds
         * of wall clock time for the first window to fire.
         *
         * Window [2026-07-20T10:00:00Z, 2026-07-20T11:00:00Z) User Id: 42 404s share: 0.1411764705882353
         */
        watermarkedStream
                .keyBy(new KeySelector<PageViewEvent, Long>() {
                    @Override
                    public Long getKey(PageViewEvent value) throws Exception {
                        return value.user_id;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new AggregateFunction<PageViewEvent, Tuple2<Long, Long>, Double>() {
                            @Override
                            public Tuple2<Long, Long> createAccumulator() {
                                return Tuple2.of(0L, 0L);
                            }

                            @Override
                            public Tuple2<Long, Long> add(PageViewEvent value, Tuple2<Long, Long> accumulator) {
                                return Tuple2.of(
                                        accumulator.f0 + ("404".equals(value.page) ? 1L : 0L),
                                        accumulator.f1 + 1L);
                            }

                            @Override
                            public Double getResult(Tuple2<Long, Long> accumulator) {
                                return accumulator.f1 == 0L ? 0.0 : (double) accumulator.f0 / accumulator.f1;
                            }

                            @Override
                            public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                            }
                        },
                        new ProcessWindowFunction<Double, String, Long, TimeWindow>() {
                            @Override
                            public void process(Long user_id, ProcessWindowFunction<Double, String, Long, TimeWindow>.Context context, Iterable<Double> elements, Collector<String> out) throws Exception {
                                out.collect("Window [" +
                                        Instant.ofEpochMilli(context.window().getStart()) +
                                        ", " +
                                        Instant.ofEpochMilli(context.window().getEnd()) +
                                        ") User Id: " +
                                        user_id +
                                        " 404s share: " +
                                        elements.iterator().next());
                            }
                        })
                .print();

        env.execute(DataStream05CountShareOf404PerUserSolution.class.getName());
    }
}
