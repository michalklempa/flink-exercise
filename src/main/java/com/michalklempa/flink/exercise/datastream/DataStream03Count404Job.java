package com.michalklempa.flink.exercise.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStream03Count404Job {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.fromSource(JsonSourceFactory.create(), WatermarkStrategy.noWatermarks(), "Json Source");

        // TODO Count 404s per minute and print counts with window start/end (processing time)

        // TODO Count 404s per minute and print counts with window start/end (event time)

        env.execute(DataStream03Count404Job.class.getName());
    }
}
;