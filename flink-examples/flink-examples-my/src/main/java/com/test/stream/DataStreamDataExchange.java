package com.test.stream;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
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

        TypeInformation<Tuple2<String, byte[]>> typeInformation = Types.TUPLE(Types.STRING, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        DataStream<Tuple2<String, byte[]>> ds = env.addSource(new CustomSourceFunction<Tuple2<String, byte[]>>() {
            int i = 0;
            @Override
            public Tuple2<String, byte[]> elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                return Tuple2.of(indexOfSubtask + "-" + i++, new byte[1 * 1]);
            }
        }, typeInformation);

        ds.filter(x -> true)
           .keyBy(x -> x.f0).map(x -> x, typeInformation)
           .addSink(new SinkFunction<Tuple2<String, byte[]>>() {
               @Override
               public void invoke(Tuple2<String, byte[]> value, Context context) throws Exception {
                   //Thread.sleep(2000);
                   LOG.warn(value.f0);
               }
           });

        env.execute("DataStreamDataExchange");
    }

}
