package com.test.stream;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.delegation.PlannerHelper;
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter;

import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank;

import org.apache.flink.table.runtime.operators.rank.window.processors.WindowRankProcessor;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public class FlinkSqlWindowQueryPlanCat {

    @Test
    public void testTumbleEventTimeWindow() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        var sql = """
                CREATE TABLE tmp_tb (
                  page_id int,
                  cnt1 int,
                  cnt2 int,
                  cnt3 int,
                  ts TIMESTAMP(3),
                  -- ts as CURRENT_TIMESTAMP,
                  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                ) WITH (
                  'connector'='datagen',
                  'rows-per-second'='2',
                  'fields.page_id.min'='1',
                  'fields.page_id.max'='2',
                  'fields.ts.max-past'='0',
                  'fields.cnt1.min'='1',
                  'fields.cnt1.max'='1',
                  'fields.cnt2.min'='1',
                  'fields.cnt2.max'='10',
                  'fields.cnt3.min'='1',
                  'fields.cnt3.max'='10'
                )
                """;
        tEnv.executeSql(sql);

        /**
         * analyzed plan string:
         * LogicalProject(exprs=[[TUMBLE_START($1), TUMBLE_END($1), TUMBLE_ROWTIME($1), $0, $2, $3, $4]])
         * +- LogicalAggregate(group=[{0, 1}], cnt1=[SUM($2)], cnt2=[SUM($3)], cnt=[COUNT()])
         *    +- LogicalProject(inputs=[0], exprs=[[$TUMBLE($4, 5000:INTERVAL SECOND), $1, $2]])
         *       +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($4, 5000:INTERVAL SECOND)])
         *          +- LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]])
         *
         * optimized plan string:
         * Calc(select=[w$start AS window_start, w$end AS window_end, w$rowtime AS row_time, page_id, cnt1, cnt2, cnt], upsertKeys=[[window_start, page_id], [row_time, page_id], [window_end, page_id]])
         * +- GroupWindowAggregate(groupBy=[page_id], window=[TumblingGroupWindow('w$, ts, 5000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[page_id, SUM(cnt1) AS cnt1, SUM(cnt2) AS cnt2, COUNT(*) AS cnt, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime], upsertKeys=[[page_id, w$proctime], [page_id, w$rowtime], [page_id, w$start], [page_id, w$end]])
         *    +- Exchange(distribution=[hash[page_id]])
         *       +- Calc(select=[page_id, ts, cnt1, cnt2])
         *          +- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)])
         *             +- TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts])
         *
         * analyzed plan digest:
         * LogicalProject(exprs=[[TUMBLE_START($1), TUMBLE_END($1), TUMBLE_ROWTIME($1), $0, $2, $3, $4]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* row_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * LogicalAggregate(group=[{0, 1}], cnt1=[SUM($2)], cnt2=[SUM($3)], cnt=[COUNT()]), rowType=[RecordType(INTEGER page_id, TIMESTAMP(3) *ROWTIME* $f1, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * LogicalProject(inputs=[0], exprs=[[$TUMBLE($4, 5000:INTERVAL SECOND), $1, $2]]), rowType=[RecordType(INTEGER page_id, TIMESTAMP(3) *ROWTIME* $f1, INTEGER cnt1, INTEGER cnt2)]
         * LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($4, 5000:INTERVAL SECOND)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         *
         * optimized plan digest:
         * Calc(select=[w$start AS window_start, w$end AS window_end, w$rowtime AS row_time, page_id, cnt1, cnt2, cnt], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* row_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * GroupWindowAggregate(groupBy=[page_id], window=[TumblingGroupWindow('w$, ts, 5000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[page_id, SUM(cnt1) AS cnt1, SUM(cnt2) AS cnt2, COUNT(*) AS cnt, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, TIMESTAMP(3) w$start, TIMESTAMP(3) w$end, TIMESTAMP(3) *ROWTIME* w$rowtime, TIMESTAMP_LTZ(3) *PROCTIME* w$proctime)]
         * Exchange(distribution=[hash[page_id]], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, TIMESTAMP(3) *ROWTIME* ts, INTEGER cnt1, INTEGER cnt2)]
         * Calc(select=[page_id, ts, cnt1, cnt2], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, TIMESTAMP(3) *ROWTIME* ts, INTEGER cnt1, INTEGER cnt2)]
         * WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         */
        sql = """
              select
                TUMBLE_START(ts, INTERVAL '5' SECOND) as window_start,
                TUMBLE_END(ts, INTERVAL '5' SECOND) as window_end,
                TUMBLE_ROWTIME(ts, INTERVAL '5' SECOND) as row_time,
                page_id,
                sum(cnt1) cnt1,
                sum(cnt2) cnt2,
                count(1) cnt
              from tmp_tb
              group by page_id, TUMBLE(ts, INTERVAL '5' SECOND)
                """;
        var rstTable = tEnv.sqlQuery(sql);
        rstTable.printSchema();
        QueryOperation queryOperation = rstTable.getQueryOperation();
        Transformation<RowData> transformation = PlannerHelper.translate(
                (AbstractStreamTableEnvironmentImpl) tEnv,
                queryOperation);
        var dsRst = new DataStream<RowData>(env, transformation);
        dsRst.addSink(new SinkFunction<RowData>() {
            @Override
            public void invoke(RowData value, Context context) throws Exception {
                System.out.println(value);
            }
        }).name("sink");

        env.execute("test");
    }

    @Test
    public void testTumbleEventTimeWindowTvfOnePhase() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
        var sql = """
                CREATE TABLE tmp_tb (
                  page_id int,
                  cnt1 int,
                  cnt2 int,
                  cnt3 int,
                  ts TIMESTAMP(3),
                  -- ts as CURRENT_TIMESTAMP,
                  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                ) WITH (
                  'connector'='datagen',
                  'rows-per-second'='2',
                  'fields.page_id.min'='1',
                  'fields.page_id.max'='2',
                  'fields.ts.max-past'='0',
                  'fields.cnt1.min'='1',
                  'fields.cnt1.max'='1',
                  'fields.cnt2.min'='1',
                  'fields.cnt2.max'='10',
                  'fields.cnt3.min'='1',
                  'fields.cnt3.max'='10'
                )
                """;
        tEnv.executeSql(sql);

        /**
         * analyzed plan string:
         * LogicalAggregate(group=[{0, 1, 2, 3}], cnt1=[SUM($4)], cnt2=[SUM($5)], cnt=[COUNT()])
         * +- LogicalProject(exprs=[[$5, $6, $7, $0, $1, $2]])
         *    +- LogicalTableFunctionScan(invocation=[TUMBLE(DESCRIPTOR($4), 5000:INTERVAL SECOND)], rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)])
         *       +- LogicalProject(inputs=[0..4])
         *          +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($4, 5000:INTERVAL SECOND)])
         *             +- LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]])
         *
         * optimized plan string:
         * Calc(select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt])
         * +- WindowAggregate(groupBy=[page_id], window=[TUMBLE(time_col=[ts], size=[5 s])], select=[page_id, SUM(cnt1) AS cnt1, SUM(cnt2) AS cnt2, COUNT(*) AS cnt, start('w$) AS window_start, end('w$) AS window_end, rowtime('w$) AS window_time])
         *    +- Exchange(distribution=[hash[page_id]])
         *       +- Calc(select=[page_id, cnt1, cnt2, ts])
         *          +- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)])
         *             +- TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts])
         *
         * analyzed plan digest:
         * LogicalAggregate(group=[{0, 1, 2, 3}], cnt1=[SUM($4)], cnt2=[SUM($5)], cnt=[COUNT()]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * LogicalProject(exprs=[[$5, $6, $7, $0, $1, $2]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2)]
         * LogicalTableFunctionScan(invocation=[TUMBLE(DESCRIPTOR($4), 5000:INTERVAL SECOND)], rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]
         * LogicalProject(inputs=[0..4]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($4, 5000:INTERVAL SECOND)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         *
         * optimized plan digest:
         * Calc(select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * WindowAggregate(groupBy=[page_id], window=[TUMBLE(time_col=[ts], size=[5 s])], select=[page_id, SUM(cnt1) AS cnt1, SUM(cnt2) AS cnt2, COUNT(*) AS cnt, start('w$) AS window_start, end('w$) AS window_end, rowtime('w$) AS window_time], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]
         * Exchange(distribution=[hash[page_id]], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, TIMESTAMP(3) *ROWTIME* ts)]
         * Calc(select=[page_id, cnt1, cnt2, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, TIMESTAMP(3) *ROWTIME* ts)]
         * WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         */
        sql = """
              select
                window_start,
                window_end,
                window_time,
                page_id,
                sum(cnt1) cnt1,
                sum(cnt2) cnt2,
                count(1) cnt
              from table( tumble(table tmp_tb, descriptor(ts), interval '5' second) )
              group by window_start, window_end, window_time, page_id
                """;
        var rstTable = tEnv.sqlQuery(sql);
        rstTable.printSchema();
        QueryOperation queryOperation = rstTable.getQueryOperation();
        Transformation<RowData> transformation = PlannerHelper.translate(
                (AbstractStreamTableEnvironmentImpl) tEnv,
                queryOperation);
        var dsRst = new DataStream<RowData>(env, transformation);
        dsRst.addSink(new SinkFunction<RowData>() {
            @Override
            public void invoke(RowData value, Context context) throws Exception {
                System.out.println(value);
            }
        }).name("sink");

        env.execute("test");
    }

    @Test
    public void testTumbleEventTimeWindowTvf() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        var sql = """
                CREATE TABLE tmp_tb (
                  page_id int,
                  cnt1 int,
                  cnt2 int,
                  cnt3 int,
                  ts TIMESTAMP(3),
                  -- ts as CURRENT_TIMESTAMP,
                  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                ) WITH (
                  'connector'='datagen',
                  'rows-per-second'='2',
                  'fields.page_id.min'='1',
                  'fields.page_id.max'='2',
                  'fields.ts.max-past'='0',
                  'fields.cnt1.min'='1',
                  'fields.cnt1.max'='1',
                  'fields.cnt2.min'='1',
                  'fields.cnt2.max'='10',
                  'fields.cnt3.min'='1',
                  'fields.cnt3.max'='10'
                )
                """;
        tEnv.executeSql(sql);

        /**
         * analyzed plan string:
         * LogicalAggregate(group=[{0, 1, 2, 3}], cnt1=[SUM($4)], cnt2=[SUM($5)], cnt=[COUNT()])
         * +- LogicalProject(exprs=[[$5, $6, $7, $0, $1, $2]])
         *    +- LogicalTableFunctionScan(invocation=[TUMBLE(DESCRIPTOR($4), 5000:INTERVAL SECOND)], rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)])
         *       +- LogicalProject(inputs=[0..4])
         *          +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($4, 5000:INTERVAL SECOND)])
         *             +- LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]])
         *
         * optimized plan string:
         * Calc(select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt])
         * +- GlobalWindowAggregate(groupBy=[page_id], window=[TUMBLE(slice_end=[$slice_end], size=[5 s])], select=[page_id, SUM(sum$0) AS cnt1, SUM(sum$1) AS cnt2, COUNT(count1$2) AS cnt, start('w$) AS window_start, end('w$) AS window_end, rowtime('w$) AS window_time])
         *    +- Exchange(distribution=[hash[page_id]])
         *       +- LocalWindowAggregate(groupBy=[page_id], window=[TUMBLE(time_col=[ts], size=[5 s])], select=[page_id, SUM(cnt1) AS sum$0, SUM(cnt2) AS sum$1, COUNT(*) AS count1$2, slice_end('w$) AS $slice_end])
         *          +- Calc(select=[page_id, cnt1, cnt2, ts])
         *             +- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)])
         *                +- TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts])
         *
         * analyzed plan digest:
         * LogicalAggregate(group=[{0, 1, 2, 3}], cnt1=[SUM($4)], cnt2=[SUM($5)], cnt=[COUNT()]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * LogicalProject(exprs=[[$5, $6, $7, $0, $1, $2]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2)]
         * LogicalTableFunctionScan(invocation=[TUMBLE(DESCRIPTOR($4), 5000:INTERVAL SECOND)], rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]
         * LogicalProject(inputs=[0..4]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($4, 5000:INTERVAL SECOND)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         *
         * optimized plan digest:
         * Calc(select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * GlobalWindowAggregate(groupBy=[page_id], window=[TUMBLE(slice_end=[$slice_end], size=[5 s])], select=[page_id, SUM(sum$0) AS cnt1, SUM(sum$1) AS cnt2, COUNT(count1$2) AS cnt, start('w$) AS window_start, end('w$) AS window_end, rowtime('w$) AS window_time], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]
         * Exchange(distribution=[hash[page_id]], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER sum$0, INTEGER sum$1, BIGINT count1$2, BIGINT $slice_end)]
         * LocalWindowAggregate(groupBy=[page_id], window=[TUMBLE(time_col=[ts], size=[5 s])], select=[page_id, SUM(cnt1) AS sum$0, SUM(cnt2) AS sum$1, COUNT(*) AS count1$2, slice_end('w$) AS $slice_end], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER sum$0, INTEGER sum$1, BIGINT count1$2, BIGINT $slice_end)]
         * Calc(select=[page_id, cnt1, cnt2, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, TIMESTAMP(3) *ROWTIME* ts)]
         * WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         */
        sql = """
              select
                window_start,
                window_end,
                window_time,
                page_id,
                sum(cnt1) cnt1,
                sum(cnt2) cnt2,
                count(1) cnt
              from table( tumble(table tmp_tb, descriptor(ts), interval '5' second) )
              group by window_start, window_end, window_time, page_id
                """;
        var rstTable = tEnv.sqlQuery(sql);
        rstTable.printSchema();
        QueryOperation queryOperation = rstTable.getQueryOperation();
        Transformation<RowData> transformation = PlannerHelper.translate(
                (AbstractStreamTableEnvironmentImpl) tEnv,
                queryOperation);
        var dsRst = new DataStream<RowData>(env, transformation);
        dsRst.addSink(new SinkFunction<RowData>() {
            @Override
            public void invoke(RowData value, Context context) throws Exception {
                System.out.println(value);
            }
        }).name("sink");

        env.execute("test");
    }

    @Test
    public void testTumbleEventTimeWindowTopNOnePhase() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
        var sql = """
                CREATE TABLE tmp_tb (
                  page_id int,
                  cnt1 int,
                  cnt2 int,
                  cnt3 int,
                  ts TIMESTAMP(3),
                  -- ts as CURRENT_TIMESTAMP,
                  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                ) WITH (
                  'connector'='datagen',
                  'rows-per-second'='1000',
                  'fields.page_id.min'='1',
                  'fields.page_id.max'='100',
                  'fields.ts.max-past'='0',
                  'fields.cnt1.min'='1',
                  'fields.cnt1.max'='1',
                  'fields.cnt2.min'='1',
                  'fields.cnt2.max'='10',
                  'fields.cnt3.min'='1',
                  'fields.cnt3.max'='10'
                )
                """;
        tEnv.executeSql(sql);

        /**
         * WindowRank 算子实现
         * @see StreamExecWindowRank#translateToPlanInternal(PlannerBase, ExecNodeConfig)
         * @see WindowRankProcessor
         *
         * analyzed plan string:
         * LogicalProject(inputs=[0..6])
         * +- LogicalFilter(condition=[<=($6, 10)])
         *    +- LogicalProject(inputs=[0..5], exprs=[[ROW_NUMBER() OVER (PARTITION BY $0, $1 ORDER BY $4 DESC NULLS LAST)]])
         *       +- LogicalProject(inputs=[0..1], exprs=[[$3, $4, $5, $6]])
         *          +- LogicalAggregate(group=[{0, 1, 2, 3}], cnt1=[SUM($4)], cnt2=[SUM($5)], cnt=[COUNT()])
         *             +- LogicalProject(exprs=[[$5, $6, $7, $0, $1, $2]])
         *                +- LogicalTableFunctionScan(invocation=[TUMBLE(DESCRIPTOR($4), 5000:INTERVAL SECOND)], rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)])
         *                   +- LogicalProject(inputs=[0..4])
         *                      +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($4, 5000:INTERVAL SECOND)])
         *                         +- LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]])
         *
         * optimized plan string:
         * Calc(select=[window_start, window_end, page_id, cnt1, cnt2, cnt, w0$o0], upsertKeys=[[w0$o0]])
         * +- WindowRank(window=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=10], partitionBy=[], orderBy=[cnt2 DESC], select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt, w0$o0], upsertKeys=[[w0$o0]])
         *    +- Exchange(distribution=[single])
         *       +- Calc(select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt])
         *          +- WindowAggregate(groupBy=[page_id], window=[TUMBLE(time_col=[ts], size=[5 s])], select=[page_id, SUM(cnt1) AS cnt1, SUM(cnt2) AS cnt2, COUNT(*) AS cnt, start('w$) AS window_start, end('w$) AS window_end, rowtime('w$) AS window_time])
         *             +- Exchange(distribution=[hash[page_id]])
         *                +- Calc(select=[page_id, cnt1, cnt2, ts])
         *                   +- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)])
         *                      +- TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts])
         *
         * analyzed plan digest:
         * LogicalProject(inputs=[0..6]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT rownum)]
         * LogicalFilter(condition=[<=($6, 10)]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT rownum)]
         * LogicalProject(inputs=[0..5], exprs=[[ROW_NUMBER() OVER (PARTITION BY $0, $1 ORDER BY $4 DESC NULLS LAST)]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT rownum)]
         * LogicalProject(inputs=[0..1], exprs=[[$3, $4, $5, $6]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * LogicalAggregate(group=[{0, 1, 2, 3}], cnt1=[SUM($4)], cnt2=[SUM($5)], cnt=[COUNT()]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * LogicalProject(exprs=[[$5, $6, $7, $0, $1, $2]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2)]
         * LogicalTableFunctionScan(invocation=[TUMBLE(DESCRIPTOR($4), 5000:INTERVAL SECOND)], rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]
         * LogicalProject(inputs=[0..4]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($4, 5000:INTERVAL SECOND)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         *
         * optimized plan digest:
         * Calc(select=[window_start, window_end, page_id, cnt1, cnt2, cnt, w0$o0], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT w0$o0)]
         * WindowRank(window=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=10], partitionBy=[], orderBy=[cnt2 DESC], select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt, w0$o0], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT w0$o0)]
         * Exchange(distribution=[single], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * Calc(select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * WindowAggregate(groupBy=[page_id], window=[TUMBLE(time_col=[ts], size=[5 s])], select=[page_id, SUM(cnt1) AS cnt1, SUM(cnt2) AS cnt2, COUNT(*) AS cnt, start('w$) AS window_start, end('w$) AS window_end, rowtime('w$) AS window_time], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]
         * Exchange(distribution=[hash[page_id]], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, TIMESTAMP(3) *ROWTIME* ts)]
         * Calc(select=[page_id, cnt1, cnt2, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, TIMESTAMP(3) *ROWTIME* ts)]
         * WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         */
        sql = """
                select
                    *
                from (
                    select
                        *,
                        row_number() over (partition by window_start, window_end order by cnt2 desc) as rownum
                    from (
                        select
                          window_start,
                          window_end,
                          page_id,
                          sum(cnt1) cnt1,
                          sum(cnt2) cnt2,
                          count(1) cnt
                        from table( tumble(table tmp_tb, descriptor(ts), interval '5' second) )
                        group by window_start, window_end, window_time, page_id
                    )
                ) where rownum <= 10
                  """;
        var rstTable = tEnv.sqlQuery(sql);
        rstTable.printSchema();
        QueryOperation queryOperation = rstTable.getQueryOperation();
        Transformation<RowData> transformation = PlannerHelper.translate(
                (AbstractStreamTableEnvironmentImpl) tEnv,
                queryOperation);
        var dsRst = new DataStream<RowData>(env, transformation);
        dsRst.addSink(new SinkFunction<RowData>() {
            @Override
            public void invoke(RowData value, Context context) throws Exception {
                System.out.println(value);
            }
        }).name("sink");

        env.execute("test");
    }

    @Test
    public void testTumbleEventTimeWindowTopN() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        var sql = """
                CREATE TABLE tmp_tb (
                  page_id int,
                  cnt1 int,
                  cnt2 int,
                  cnt3 int,
                  ts TIMESTAMP(3),
                  -- ts as CURRENT_TIMESTAMP,
                  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                ) WITH (
                  'connector'='datagen',
                  'rows-per-second'='1000',
                  'fields.page_id.min'='1',
                  'fields.page_id.max'='100',
                  'fields.ts.max-past'='0',
                  'fields.cnt1.min'='1',
                  'fields.cnt1.max'='1',
                  'fields.cnt2.min'='1',
                  'fields.cnt2.max'='10',
                  'fields.cnt3.min'='1',
                  'fields.cnt3.max'='10'
                )
                """;
        tEnv.executeSql(sql);

        /**
         * optimized plan string:
         * Calc(select=[window_start, window_end, page_id, cnt1, cnt2, cnt, w0$o0], upsertKeys=[[w0$o0]])
         * +- WindowRank(window=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=10], partitionBy=[], orderBy=[cnt2 DESC], select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt, w0$o0], upsertKeys=[[w0$o0]])
         *    +- Exchange(distribution=[single])
         *       +- Calc(select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt])
         *          +- GlobalWindowAggregate(groupBy=[page_id], window=[TUMBLE(slice_end=[$slice_end], size=[5 s])], select=[page_id, SUM(sum$0) AS cnt1, SUM(sum$1) AS cnt2, COUNT(count1$2) AS cnt, start('w$) AS window_start, end('w$) AS window_end, rowtime('w$) AS window_time])
         *             +- Exchange(distribution=[hash[page_id]])
         *                +- LocalWindowAggregate(groupBy=[page_id], window=[TUMBLE(time_col=[ts], size=[5 s])], select=[page_id, SUM(cnt1) AS sum$0, SUM(cnt2) AS sum$1, COUNT(*) AS count1$2, slice_end('w$) AS $slice_end])
         *                   +- Calc(select=[page_id, cnt1, cnt2, ts])
         *                      +- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)])
         *                         +- TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts])
         *
         * analyzed plan digest:
         * LogicalProject(inputs=[0..6]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT rownum)]
         * LogicalFilter(condition=[<=($6, 10)]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT rownum)]
         * LogicalProject(inputs=[0..5], exprs=[[ROW_NUMBER() OVER (PARTITION BY $0, $1 ORDER BY $4 DESC NULLS LAST)]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT rownum)]
         * LogicalProject(inputs=[0..1], exprs=[[$3, $4, $5, $6]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * LogicalAggregate(group=[{0, 1, 2, 3}], cnt1=[SUM($4)], cnt2=[SUM($5)], cnt=[COUNT()]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * LogicalProject(exprs=[[$5, $6, $7, $0, $1, $2]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2)]
         * LogicalTableFunctionScan(invocation=[TUMBLE(DESCRIPTOR($4), 5000:INTERVAL SECOND)], rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]
         * LogicalProject(inputs=[0..4]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($4, 5000:INTERVAL SECOND)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         *
         * optimized plan digest:
         * Calc(select=[window_start, window_end, page_id, cnt1, cnt2, cnt, w0$o0], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT w0$o0)]
         * WindowRank(window=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=10], partitionBy=[], orderBy=[cnt2 DESC], select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt, w0$o0], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, BIGINT w0$o0)]
         * Exchange(distribution=[single], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * Calc(select=[window_start, window_end, window_time, page_id, cnt1, cnt2, cnt], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time, INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt)]
         * GlobalWindowAggregate(groupBy=[page_id], window=[TUMBLE(slice_end=[$slice_end], size=[5 s])], select=[page_id, SUM(sum$0) AS cnt1, SUM(sum$1) AS cnt2, COUNT(count1$2) AS cnt, start('w$) AS window_start, end('w$) AS window_end, rowtime('w$) AS window_time], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, BIGINT cnt, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)]
         * Exchange(distribution=[hash[page_id]], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER sum$0, INTEGER sum$1, BIGINT count1$2, BIGINT $slice_end)]
         * LocalWindowAggregate(groupBy=[page_id], window=[TUMBLE(time_col=[ts], size=[5 s])], select=[page_id, SUM(cnt1) AS sum$0, SUM(cnt2) AS sum$1, COUNT(*) AS count1$2, slice_end('w$) AS $slice_end], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER sum$0, INTEGER sum$1, BIGINT count1$2, BIGINT $slice_end)]
         * Calc(select=[page_id, cnt1, cnt2, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, TIMESTAMP(3) *ROWTIME* ts)]
         * WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) *ROWTIME* ts)]
         * TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt1, cnt2, cnt3, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt1, INTEGER cnt2, INTEGER cnt3, TIMESTAMP(3) ts)]
         */
        sql = """
                select
                    *
                from (
                    select
                        *,
                        row_number() over (partition by window_start, window_end order by cnt2 desc) as rownum
                    from (
                        select
                          window_start,
                          window_end,
                          page_id,
                          sum(cnt1) cnt1,
                          sum(cnt2) cnt2,
                          count(1) cnt
                        from table( tumble(table tmp_tb, descriptor(ts), interval '5' second) )
                        group by window_start, window_end, window_time, page_id
                    )
                ) where rownum <= 10
                  """;
        var rstTable = tEnv.sqlQuery(sql);
        rstTable.printSchema();
        QueryOperation queryOperation = rstTable.getQueryOperation();
        Transformation<RowData> transformation = PlannerHelper.translate(
                (AbstractStreamTableEnvironmentImpl) tEnv,
                queryOperation);
        var dsRst = new DataStream<RowData>(env, transformation);
        dsRst.addSink(new SinkFunction<RowData>() {
            @Override
            public void invoke(RowData value, Context context) throws Exception {
                System.out.println(value);
            }
        }).name("sink");

        env.execute("test");
    }

}
