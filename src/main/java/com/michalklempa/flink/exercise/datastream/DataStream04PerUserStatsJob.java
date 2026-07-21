package com.michalklempa.flink.exercise.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStream04PerUserStatsJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.fromSource(JsonSourceFactory.create(), WatermarkStrategy.noWatermarks(), "Json Source");

        // TODO Count per-user page views per minute and print counts with window start/end (processing time)

        // TODO Count per-user page views per minute and print counts with window start/end (event time)

        env.execute(DataStream04PerUserStatsJob.class.getName());
    }
}
;