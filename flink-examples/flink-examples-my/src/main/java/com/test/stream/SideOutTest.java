package com.test.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 有多个Outputs时会使用BroadcastingOutputCollector输出，BroadcastingOutputCollector内部会遍历Outputs数组输出
 * 当开始对象重用时创建CopyingBroadcastingOutputCollector，否则BroadcastingOutputCollector，差别不大，就是复制一个新的StreamRecord，浅拷贝，不会拷贝value
 * 一下两者情况效果基本是一样的，性能也基本是一样的，具体可以看BroadcastingOutputCollector/CopyingBroadcastingOutputCollector的collect(outputTag, record),collect(record)方法：
 *   使用SideOut，可能调用collect(outputTag, record)或则collect(record)方法
 *   一个节点输出连接两个节点，调用collect(record)方法，这里可以在下游过滤实现的效果和使用SideOut没什么区别
 *
 * @see OperatorChain#createOutputCollector(StreamTask, StreamConfig, Map, ClassLoader, Map, List, MailboxExecutorFactory, boolean)
 */
public class SideOutTest {
    private static final Logger LOG = LoggerFactory.getLogger(SideOutTest.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString("heartbeat.timeout", "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);

        //linkFilterSink(env);
        //linkSink(env);
        //linkMapSink(env);
        //sideOut(env);
        //twoInputsUseOneSource(env);
        twoInputsUseTwoSource(env);

        env.execute("SideOutTest");
    }

    public static void linkFilterSink(StreamExecutionEnvironment env){
        DataStream<Tuple2<Integer, Integer>> ds = env.addSource(new CustomSourceFunction<Tuple2<Integer, Integer>>() {
            Tuple2<Integer, Integer> data = new Tuple2<>();
            int i = 0;

            @Override
            public Tuple2<Integer, Integer> elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                i++;
                data.f0 = getRuntimeContext().getIndexOfThisSubtask();
                data.f1 = i;
                return data;
            }
        });

        ds.filter(x -> x.f1 % 2 == 0).addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink1:" + value);;
            }
        });

        ds.filter(x -> x.f1 % 2 == 1).addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink2:" + value);;
            }
        });
    }

    public static void linkSink(StreamExecutionEnvironment env){
        DataStream<Tuple2<Integer, Integer>> ds = env.addSource(new CustomSourceFunction<Tuple2<Integer, Integer>>() {
            Tuple2<Integer, Integer> data = new Tuple2<>();
            int i = 0;

            @Override
            public Tuple2<Integer, Integer> elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                i++;
                data.f0 = getRuntimeContext().getIndexOfThisSubtask();
                data.f1 = i;
                return data;
            }
        });

        ds.addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink1:" + value);;
            }
        });

        ds.addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink2:" + value);;
            }
        });
    }

    public static void linkMapSink(StreamExecutionEnvironment env){
        DataStream<Tuple2<Integer, Integer>> ds = env.addSource(new CustomSourceFunction<Tuple2<Integer, Integer>>() {
            Tuple2<Integer, Integer> data = new Tuple2<>();
            int i = 0;

            @Override
            public Tuple2<Integer, Integer> elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                i++;
                data.f0 = getRuntimeContext().getIndexOfThisSubtask();
                data.f1 = i;
                return data;
            }
        }).map(x -> x, Types.TUPLE(Types.INT, Types.INT));

        ds.map(x -> x, Types.TUPLE(Types.INT, Types.INT)).name("map1").addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink1:" + value);;
            }
        });

        ds.map(x -> x, Types.TUPLE(Types.INT, Types.INT)).name("map2").addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink2:" + value);;
            }
        });
    }

    public static void sideOut(StreamExecutionEnvironment env){
        OutputTag<Tuple2<Integer, Integer>> outputTag = new OutputTag<Tuple2<Integer, Integer>>("side-output") {};
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> ds = env.addSource(new CustomSourceFunction<Tuple2<Integer, Integer>>() {
            Tuple2<Integer, Integer> data = new Tuple2<>();
            int i = 0;

            @Override
            public Tuple2<Integer, Integer> elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                i++;
                data.f0 = getRuntimeContext().getIndexOfThisSubtask();
                data.f1 = i;
                return data;
            }
        }).process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public void processElement( Tuple2<Integer, Integer> value, ProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>.Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                ctx.output(outputTag, value);
                out.collect(value);
            }
        });

        ds.getSideOutput(outputTag).map(x -> x, Types.TUPLE(Types.INT, Types.INT)).name("map1").addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink1:" + value);;
            }
        });

        ds.map(x -> x, Types.TUPLE(Types.INT, Types.INT)).name("map2").addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink2:" + value);;
            }
        });
    }

    /**
     * 两个chain， 第一个chain有两个output
     */
    public static void twoInputsUseOneSource(StreamExecutionEnvironment env){
        DataStream<Tuple2<Integer, Integer>> ds = env.addSource(new CustomSourceFunction<Tuple2<Integer, Integer>>() {
            Tuple2<Integer, Integer> data = new Tuple2<>();
            int i = 0;

            @Override
            public Tuple2<Integer, Integer> elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                i++;
                data.f0 = getRuntimeContext().getIndexOfThisSubtask();
                data.f1 = i;
                return data;
            }
        }).map(x -> x, Types.TUPLE(Types.INT, Types.INT));

        ds.union(ds).map(x -> x, Types.TUPLE(Types.INT, Types.INT)).name("map1").addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink1:" + value);;
            }
        });
    }

    /**
     * 三个个chain，前两个chain的output指向第三个chain
     */
    public static void twoInputsUseTwoSource(StreamExecutionEnvironment env){
        DataStream<Tuple2<Integer, Integer>> ds = env.addSource(new CustomSourceFunction<Tuple2<Integer, Integer>>() {
            Tuple2<Integer, Integer> data = new Tuple2<>();
            int i = 0;

            @Override
            public Tuple2<Integer, Integer> elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                i++;
                data.f0 = getRuntimeContext().getIndexOfThisSubtask();
                data.f1 = i;
                return data;
            }
        }).map(x -> x, Types.TUPLE(Types.INT, Types.INT));

        DataStream<Tuple2<Integer, Integer>> ds2 = env.addSource(new CustomSourceFunction<Tuple2<Integer, Integer>>() {
            Tuple2<Integer, Integer> data = new Tuple2<>();
            int i = 0;

            @Override
            public Tuple2<Integer, Integer> elementGene() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                i++;
                data.f0 = getRuntimeContext().getIndexOfThisSubtask();
                data.f1 = i;
                return data;
            }
        }).map(x -> x, Types.TUPLE(Types.INT, Types.INT));

        ds.union(ds2).map(x -> x, Types.TUPLE(Types.INT, Types.INT)).name("map1").addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                LOG.warn("sink1:" + value);;
            }
        });
    }
}
