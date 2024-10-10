package com.test.stream;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class SimpleEtlTest {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleEtlTest.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new CustomSourceFunction<Tuple2<String, Integer>>() {
            String[] array = new String[]{"a", "b", "c", "d"};
            int i = 0;
            @Override
            public Tuple2<String, Integer> elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                i++;
                return Tuple2.of(array[ThreadLocalRandom.current().nextInt(array.length)], i % 10);
            }
        });

        DataStream<String> rstDs = ds.filter(new RichFilterFunction<Tuple2<String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                LOG.warn("filter-open");;
            }

            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                boolean flag = value.f1 % 2 == 0;
                return flag;
            }

            @Override
            public void close() throws Exception {
                LOG.warn("filter-close");
            }
        }).map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                LOG.warn("map-open");
            }

            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.toString();
            }

            @Override
            public void close() throws Exception {
                LOG.warn("map-close");
            }
        });

        rstDs.addSink(new RichSinkFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                LOG.warn("sink-open");
            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                LOG.warn(value);
            }

            @Override
            public void close() throws Exception {
                LOG.warn("sink-close");
            }
        });

        env.execute("SimpleEtlTest");
    }

}
