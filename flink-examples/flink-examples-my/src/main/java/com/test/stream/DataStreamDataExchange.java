package com.test.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamDataExchange {
    static final Logger LOG = LoggerFactory.getLogger(DataStreamDataExchange.class);

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("heartbeat.timeout", "300000");
        configuration.setString("execution.buffer-timeout.interval", "5000ms");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new CustomSourceFunction<String>() {
            int i = 0;
            @Override
            public String elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                return indexOfSubtask + "-" + i++;
            }
        });

        ds.filter(x -> true)
           .keyBy(x -> x).map(x -> x)
           .addSink(new SinkFunction<String>() {
               @Override
               public void invoke(String value, Context context) throws Exception {
                   LOG.warn(value);
               }
           });

        env.execute("DataStreamDataExchange");
    }

}
