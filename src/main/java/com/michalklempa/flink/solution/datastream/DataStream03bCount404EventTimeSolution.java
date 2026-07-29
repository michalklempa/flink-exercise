package com.michalklempa.flink.solution.datastream;

import com.michalklempa.flink.exercise.datastream.PageViewEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.stream.StreamSupport;

public class DataStream03bCount404EventTimeSolution {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.fromSource(JsonSourceFactory.create(), WatermarkStrategy.noWatermarks(), "Json Source");

        DataStream<PageViewEvent> pageNotFoundStream = inputStream
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
                });

        /**
         * Solution of "count of all 404s per minute" using windowAll and aggregate in event time.
         *
         * It is identical to processing time solution, we just assign watermarks from data prior
         * the actual windowing.
         */
        DataStream<PageViewEvent> watermarkedStream = pageNotFoundStream.assignTimestampsAndWatermarks(
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
         * Solution of "count of all 404s per minute" using windowAll and aggregate in event time.
         *
         * We must use TumblingEventTimeWindows.
         *
         * Notice the output - timestamps of windows are from long time ago (the actual data timestamp):
         * Window [2026-07-20T10:06:00Z, 2026-07-20T10:07:00Z) 404s count: 20
         *
         * And for some output to appear, we do not need to wait 1 minute.
         */
        watermarkedStream
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new AggregateFunction<PageViewEvent, Long, Long>() {
                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(PageViewEvent value, Long accumulator) {
                                   return accumulator + 1;
                               }

                               @Override
                               public Long getResult(Long accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public Long merge(Long a, Long b) {
                                   return a + b;
                               }
                           },
                        new ProcessAllWindowFunction<Long, String, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<Long, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                                out.collect("Window [" +
                                        Instant.ofEpochMilli(context.window().getStart()) +
                                        ", " +
                                        Instant.ofEpochMilli(context.window().getEnd()) +
                                        ") 404s count: " +
                                        elements.iterator().next());
                            }
                        })
                .print();

        env.execute(DataStream03bCount404EventTimeSolution.class.getName());
    }
}
;
