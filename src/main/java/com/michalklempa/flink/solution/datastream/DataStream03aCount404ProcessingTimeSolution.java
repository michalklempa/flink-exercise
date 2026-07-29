package com.michalklempa.flink.solution.datastream;

import com.michalklempa.flink.exercise.datastream.PageViewEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.stream.StreamSupport;

public class DataStream03aCount404ProcessingTimeSolution {

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
         * Solution of "count of all 404s per minute" using windowAll and ProcessAllWindowFunction
         *
         * Notice the elements of the window must be kept around in memory until the window trigger
         * fires. Even though we only need the count value.
         *
         * Notice that output timestamps represent current wall clock time:
         * Processing Time - Window [2026-07-29T14:45:00Z, 2026-07-29T14:46:00Z) 404s count: 422
         */
        pageNotFoundStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new ProcessAllWindowFunction<PageViewEvent, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<PageViewEvent, String, TimeWindow>.Context context, Iterable<PageViewEvent> elements, Collector<String> out) throws Exception {
                        long count = 0;
                        /**
                         * C-style for loop solution
                         */
                        for (PageViewEvent ignored : elements) {
                            count++;
                        }
                        /**
                         * More Java-11ish solution using streams
                         */
                        count = StreamSupport.stream(elements.spliterator(), false).count();
                        TimeWindow window = context.window();
                        out.collect("Processing Time - Window [" + window.getStart() + ", " + window.getEnd() + "): 404s count: " + count);
                    }
                });
//                .print();

        /**
         * Solution of "count of all 404s per minute" using windowAll and .reduce
         *
         * Notice that to use a Reduce(A, B) -> C, the A, B and C must be of same type.
         * To make this work we can either abuse the PageViewEvent - add a field count.
         * Or we can create a separate entity class, in this case, as we only need the
         * final number, that would be Long.
         *
         * Notice that this only prints the numbers, we have lost the window context
         * and "page".
         */
        pageNotFoundStream
                .map(x -> 1L) // Convert the values to Long
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .reduce((a, b) -> a + b);
//                .print();


        /**
         * Solution of "count of all 404s per minute" using windowAll and aggregate
         * to avoid conversion to Long using map before the aggregation.
         *
         * Notice that to use a Aggrega(A, B) -> C, the A, B must be of same type -
         * input stream data type, the output C and the accumulator ACC can be of different
         * type.
         *
         * This also prints just the numbers!
         */
        pageNotFoundStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
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
                });
//                .print();

        /**
         * Solution of "count of all 404s per minute" using windowAll and aggregate
         * to avoid conversion to Long using map before the aggregation and WindowFunction
         * to actually capture the Window boundaries.
         *
         */
        pageNotFoundStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
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

        env.execute(DataStream03aCount404ProcessingTimeSolution.class.getName());
    }
}
;
