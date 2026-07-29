package com.michalklempa.flink.exercise.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStream01DeserializeJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.fromSource(JsonSourceFactory.create(), WatermarkStrategy.noWatermarks(), "Json Source");

        /**
         * TODO deserialize JSON using .map() on inputStream
         *
         * DataStream<ClickEvent> clickEventStream= inputStream.map( TODO )
         *
         * Resources:
         * - DataStream > Operators > [map](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/#map)
         */

        //
        // TODO print clickEventStream POJOs

        env.execute(DataStream01DeserializeJob.class.getName());
    }
}
