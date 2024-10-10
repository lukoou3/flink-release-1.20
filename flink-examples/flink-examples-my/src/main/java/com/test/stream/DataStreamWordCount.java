package com.test.stream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * WordCount, 最基础的flink程序
 */
public class DataStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> textDs = env.addSource(new CustomSourceFunction<String>() {
            @Override
            public String elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                return "a b c";
            }
        });

        DataStream<Tuple2<String, Integer>> rstDs = textDs.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\\s+");
                for (int i = 0; i < words.length; i++) {
                    out.collect(Tuple2.of(words[i], 1));
                }
            }
        }).filter(x -> !x.f0.isEmpty()).keyBy(x -> x.f0).sum(1);

        rstDs.print();

        env.execute("DataStreamWordCount");
    }

}
