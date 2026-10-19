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
         * Solution with ObjectMapper per item
         */
        inputStream
                /**
                 * BAD: a new ObjectMapper is instantiated for every single record.
                 * ObjectMapper is expensive to create: it builds its serializer/deserializer factories,
                 * module registrations and type resolution setup from scratch.
                 * More importantly, it caches resolved JavaType and JsonDeserializer instances internally,
                 * so a fresh instance throws the cache away and re-introspects PageViewEvent via reflection
                 * on every call. At 60 events/second this is wasteful, and on a real high-throughput stream
                 * it causes high CPU usage, heavy allocation and garbage collection pressure, and latency spikes.
                 * ObjectMapper is thread-safe once configured, so it should be created once and reused,
                 * either as a serialized field or, preferably, in RichMapFunction.open().
                 */
                .map(value -> {
                    ObjectMapper om = new ObjectMapper();
                    return om.readValue(value, PageViewEvent.class);
                })
                .print();


        /**
         * Solution with static objectMapper
         */

        inputStream
                /**
                 * The objectMapper is a static field, so the lambda does not capture it and nothing is serialized.
                 * Each TaskManager JVM initializes its own instance when the class is loaded.
                 * This works, but it silently breaks down if the mapper becomes an instance field or a captured local variable,
                 * because then it is serialized together with the function.
                 */
                .map(value -> objectMapper.readValue(value, PageViewEvent.class))
                .print();

        /**
         * Solution with serialized objectMapper
         */
        final ObjectMapper capturedObjectMapper = new ObjectMapper();
        inputStream
                /**
                 * The lambda captures the local variable, so Flink Java-serializes the objectMapper with the function
                 * and ships it to executors, where it is deserialized with empty internal caches.
                 * Everything registered on the mapper (modules, serializers, handlers) must be Serializable,
                 * otherwise job submission fails with a "not serializable" error.
                 */
                .map(value -> capturedObjectMapper.readValue(value, PageViewEvent.class))
                .print();

        /**
         * Solution with RichMapFunction and open() lifecycle
         *
         * The idiomatic Flink way. Heavy resources stay transient and are created on the TaskManager in open().
         * The mapper is built once per task and reused, need not be Serializable.
         * A matching close() can release resources.
         *
         * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/user_defined_functions/
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
