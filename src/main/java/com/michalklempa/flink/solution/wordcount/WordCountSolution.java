package com.michalklempa.flink.solution.wordcount;

import com.michalklempa.flink.exercise.wordcount.SentenceSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountSolution {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> textStream = env.fromSource(SentenceSourceFactory.create(), WatermarkStrategy.noWatermarks(), "Sentence Source");

        textStream
                .flatMap((String sentence, org.apache.flink.util.Collector<String> out) -> {
                    for (String word : sentence.toLowerCase().split("\\s+")) {
                        out.collect(word);
                    }
                }, Types.STRING)
                .map(word -> Tuple2.of(word, 1L), Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .print();

        env.execute(WordCountSolution.class.getName());
    }
}

