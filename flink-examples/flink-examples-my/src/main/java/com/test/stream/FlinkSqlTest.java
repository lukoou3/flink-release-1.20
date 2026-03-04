package com.test.stream;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink;
import org.apache.flink.table.planner.plan.nodes.calcite.Sink;


import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public class FlinkSqlTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        var sql = """
                CREATE TEMPORARY TABLE heros (
                  `name` STRING,
                  `power` STRING,
                  `age` INT
                ) WITH (
                  'connector' = 'datagen',
                  'fields.name.length' = '4',
                  'fields.power.length' = '6',
                  'fields.age.min' = '10',
                  'fields.age.max' = '60',
                  'rows-per-second' = '1'
                )
                """;
        tEnv.executeSql(sql);

        /**
         * @see RelNode             就是LogicalPlan
         * @see SingleRel           就是UnaryNode LogicalPlan
         * @see Project             就是Project LogicalPlan
         * @see Filter              就是Filter LogicalPlan
         * @see LogicalProject      Project的实现, 不针对任何特定引擎
         * @see LogicalFilter       Filter的实现, 不针对任何特定引擎
         *
         *
         * @see TableScan           source表 接口
         * @see LogicalTableScan    source表
         * @see Sink                sink 接口
         * @see LogicalSink         sink, 持有DynamicTableSink, DynamicTableSink就是flink sql定义产生sink的接口
         *                                DynamicTableSinkFactory接口定义要求返回DynamicTableSink
         */
        sql = """
                select name,`power`,age,age + 10 age2 from heros
                """;

        var rstTable = tEnv.sqlQuery(sql);
        DataStream<RowData> ds = tEnv.toDataStream(
                rstTable,
                rstTable.getSchema().toRowDataType().bridgedTo(RowData.class));
        ds.addSink(new SinkFunction<RowData>() {
            @Override
            public void invoke(RowData value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("test");
    }

}
