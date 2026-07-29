package com.michalklempa.flink.solution.datastream;

import com.michalklempa.flink.exercise.datastream.PageViewEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStream01DeserializeSolution {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.fromSource(JsonSourceFactory.create(), WatermarkStrategy.noWatermarks(), "Json Source");

        /**
         * Solution with serialized objectMapper
         */

        inputStream
                /**
                 * This objectMapper undergoes Serialization to distribute itself to executors
                 */
                .map(value -> objectMapper.readValue(value, PageViewEvent.class))
                .print();

        /**
         * Solution with RichMapFunction and open() lifecycle
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
                .print();

        env.execute(DataStream01DeserializeSolution.class.getName());
    }
}
