package com.michalklempa.flink.exercise.wordcount;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> textStream = env.fromSource(SentenceSourceFactory.create(), WatermarkStrategy.noWatermarks(), "Sentence Source");

        // TODO Split sentences into words, count word occurrences, and print running counts

        env.execute(WordCountJob.class.getName());
    }
}
