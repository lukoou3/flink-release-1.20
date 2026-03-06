package com.test.stream;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.delegation.PlannerHelper;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink;
import org.apache.flink.table.planner.plan.nodes.calcite.Sink;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraphGenerator;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.junit.jupiter.api.Test;


import java.util.List;

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

        sql = """
            CREATE TABLE tmp_sink (
              `name` STRING,
              `power` STRING,
              `age` INT,
              `age2` INT
            ) WITH (
              'connector' = 'print'
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
                insert into tmp_sink
                select name,`power`,age,age + 10 age2 from heros
                where age <= 50
                """;

        /**
         * 转换的核心实现在:
         * @see PlannerBase#translate(List)
         *     // RelNode => RelNode(FlinkPhysicalRel), 优化 逻辑计划转为物理计划
         *     val optimizedRelNodes = optimize(relNodes)
         *     // RelNode(FlinkPhysicalRel) => ExecNodeGraph(ExecNode), 物理计划转为ExecNode
         *     val execGraph = translateToExecNodeGraph(optimizedRelNodes, isCompiled = false)
         *     // ExecNodeGraph(ExecNode), => Transformation(Transformation), 把ExecNode转为Transformation
         *     val transformations = translateToPlan(execGraph)
         *
         * 如果要自己实现sql到dataStream转换时，可以直接使用flink解析到优化的RelNode(FlinkPhysicalRel)，然后自己转换为ProcessFunction
         * 转换可以参考RelNode(FlinkPhysicalRel) => ExecNodeGraph(ExecNode)步骤，实际就是每个FlinkPhysicalRel实现类的translateToExecNode()方法
         *   @see org.apache.flink.table.planner.delegation.PlannerBase#translateToExecNodeGraph(scala.collection.Seq, boolean)
         *   @see ExecNodeGraphGenerator#generate(List, boolean)
         *   @see ExecNodeGraphGenerator#generate(FlinkPhysicalRel, boolean)
         *   @see FlinkPhysicalRel#translateToExecNode(boolean)
         *   @see FlinkPhysicalRel#translateToExecNode()  每个FlinkPhysicalRel实现类实现这个方法，返回ExecNode，核心实现
         *
         * 下面列出这个sql执行转换逻辑：
         *  RelNode 逻辑计划：
         *   rel#4:LogicalSink.NONE.any.None: 0.[NONE].[NONE](input=LogicalProject#3,table=default_catalog.default_database.tmp_sink,fields=name, power, age, age2)
         *    rel#3:LogicalProject.NONE.any.None: 0.[NONE].[NONE](input=LogicalFilter#2,inputs=0..2,exprs=[+($2, 10)])
         *     rel#2:LogicalFilter.NONE.any.None: 0.[NONE].[NONE](input=LogicalTableScan#1,condition=<=($2, 50))
         *      rel#1:LogicalTableScan.NONE.any.None: 0.[NONE].[NONE](table=[default_catalog, default_database, heros])
         *
         *  RelNode(FlinkPhysicalRel) 优化后的物理计划：
         *   rel#232:StreamPhysicalSink.STREAM_PHYSICAL.any.None: 0.[NONE].[NONE](input=StreamPhysicalCalc#230,table=default_catalog.default_database.tmp_sink,fields=name, power, age, age2)
         *    rel#230:StreamPhysicalCalc.STREAM_PHYSICAL.any.None: 0.[I].[NONE](input=StreamPhysicalTableSourceScan#220,select=name, power, age, +(age, 10) AS age2,where=<=(age, 50))
         *     rel#220:StreamPhysicalTableSourceScan.STREAM_PHYSICAL.any.None: 0.[I].[NONE](table=[default_catalog, default_database, heros],fields=name, power, age)
         *
         *  ExecNode 执行信息：
         *   StreamExecSink:
         *     description = Sink(table=[default_catalog.default_database.tmp_sink], fields=[name, power, age, age2])
         *     tableSinkSpec = DynamicTableSinkSpec{contextResolvedTable=default_catalog.default_database.tmp_sink, sinkAbilities=[], targetColumns=null, tableSink=org.apache.flink.connector.print.table.PrintTableSinkFactory$PrintSink@60928a61}
         *
         *   StreamExecCalc:
         *     description = Calc(select=[name, power, age, (age + 10) AS age2], where=[(age <= 50)])
         *     projection = [$0, $1, $2, +($2, 10)], condition = <=($2, 50)
         *
         *   StreamExecTableSourceScan:
         *     description = TableSourceScan(table=[[default_catalog, default_database, heros]], fields=[name, power, age])
         *     tableSourceSpec = DynamicTableSourceSpec{contextResolvedTable=default_catalog.default_database.heros, sourceAbilities=[], tableSource=org.apache.flink.connector.datagen.table.DataGenTableSource@63e4484d}
         *
         *
         */
        TableResult tableResult = tEnv.executeSql(sql);
        System.out.println(tableResult);

        env.execute("test");
    }

    @Test
    public void testFromDataStream() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        CatalogManager catalogManager = ((StreamTableEnvironmentImpl)tEnv).getCatalogManager();
        DataTypeFactory dataTypeFactory = catalogManager.getDataTypeFactory();
        InternalTypeInfo<RowData> typeInformation = InternalTypeInfo.of(dataTypeFactory.createDataType("ROW<name string, age int, cnt bigint>").getLogicalType());
        SingleOutputStreamOperator<RowData> rowDataDs = env.fromSequence(1, 10000).map(i -> {
            if (i % 5 > 0) {
                RowData row = GenericRowData.of(StringData.fromString(Long.toString(i % 4)), Integer.valueOf((int) (i % 100)), Long.valueOf(i % 100));
                return row;
            } else {
                return GenericRowData.of(null, null, null);
            }
        }, typeInformation);

        //tEnv.createTemporaryTable("SourceTableB", sourceDescriptor);

        /**
         * @see ExternalDynamicSource
         * @see InputConversionOperator
         * @see IdentityConverter
         */
        var rstTable = tEnv.fromDataStream(rowDataDs);
        rstTable.printSchema();
        QueryOperation queryOperation = rstTable.getQueryOperation();
        Transformation<RowData> transformation = PlannerHelper.translate((AbstractStreamTableEnvironmentImpl) tEnv, queryOperation);
        var dsRst = new DataStream<RowData>(env, transformation);
        dsRst.addSink(new SinkFunction<RowData>() {
            @Override
            public void invoke(RowData value, Context context) throws Exception {
                Thread.sleep(1000);
                System.out.println(value);
            }
        }).name("sink");
        env.execute("test");
    }

}
