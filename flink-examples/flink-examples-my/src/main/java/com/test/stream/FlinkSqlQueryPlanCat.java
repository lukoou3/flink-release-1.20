package com.test.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.delegation.PlannerHelper;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public class FlinkSqlQueryPlanCat {

    public static void main(String[] args) throws Exception {
    }

    @Test
    public void testBaseQuery() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                conf);
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
         * analyzed plan string:
         * LogicalProject(inputs=[0..2], exprs=[[+($2, 10)]])
         * +- LogicalFilter(condition=[<=($2, 50)])
         *    +- LogicalTableScan(table=[[default_catalog, default_database, heros]])
         *
         * optimized plan string:
         * Calc(select=[name, power, age, +(age, 10) AS age2], where=[<=(age, 50)])
         * +- TableSourceScan(table=[[default_catalog, default_database, heros]], fields=[name, power, age])
         *
         * analyzed plan digest:
         * LogicalProject(inputs=[0..2], exprs=[[+($2, 10)]]), rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) power, INTEGER age, INTEGER age2)]
         * LogicalFilter(condition=[<=($2, 50)]), rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) power, INTEGER age)]
         * LogicalTableScan(table=[[default_catalog, default_database, heros]]), rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) power, INTEGER age)]
         *
         * optimized plan digest:
         * Calc(select=[name, power, age, +(age, 10) AS age2], where=[<=(age, 50)], changelogMode=[I]), rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) power, INTEGER age, INTEGER age2)]
         * TableSourceScan(table=[[default_catalog, default_database, heros]], fields=[name, power, age], changelogMode=[I]), rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) power, INTEGER age)]
         */
        sql = """
                select name,`power`,age,age + 10 age2 from heros
                where age <= 50
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

        env.execute("testBaseQuery");
    }

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
                  cnt int,
                  ts TIMESTAMP(3),
                  -- ts as CURRENT_TIMESTAMP,
                  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                ) WITH (
                  'connector'='datagen',
                  'rows-per-second'='1',
                  'fields.page_id.min'='1',
                  'fields.page_id.max'='2',
                  'fields.ts.max-past'='0',
                  'fields.cnt.min'='1',
                  'fields.cnt.max'='1'
                )
                """;
        tEnv.executeSql(sql);

        /**
         * analyzed plan string:
         * LogicalProject(exprs=[[TUMBLE_START($1), TUMBLE_END($1), $0, $2, $3, $4]])
         * +- LogicalAggregate(group=[{0, 1}], cnt=[SUM($2)], cnt2=[SUM($3)], count2=[COUNT()])
         *    +- LogicalProject(inputs=[0], exprs=[[$TUMBLE($2, 5000:INTERVAL SECOND), $1, $3]])
         *       +- LogicalProject(inputs=[0..2], exprs=[[+($1, 10)]])
         *          +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($2, 5000:INTERVAL SECOND)])
         *             +- LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]])
         *
         * optimized plan string:
         * Calc(select=[w$start AS window_start, w$end AS window_end, page_id, cnt, cnt2, count2], upsertKeys=[[window_start, page_id], [window_end, page_id]])
         * +- GroupWindowAggregate(groupBy=[page_id], window=[TumblingGroupWindow('w$, ts, 5000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[page_id, SUM(cnt) AS cnt, SUM($f3) AS cnt2, COUNT(*) AS count2, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime], upsertKeys=[[page_id, w$proctime], [page_id, w$rowtime], [page_id, w$start], [page_id, w$end]])
         *    +- Exchange(distribution=[hash[page_id]])
         *       +- Calc(select=[page_id, ts, cnt, +(cnt, 10) AS $f3])
         *          +- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)])
         *             +- TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt, ts])
         *
         * analyzed plan digest:
         * LogicalProject(exprs=[[TUMBLE_START($1), TUMBLE_END($1), $0, $2, $3, $4]]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt, INTEGER cnt2, BIGINT count2)]
         * LogicalAggregate(group=[{0, 1}], cnt=[SUM($2)], cnt2=[SUM($3)], count2=[COUNT()]), rowType=[RecordType(INTEGER page_id, TIMESTAMP(3) *ROWTIME* $f1, INTEGER cnt, INTEGER cnt2, BIGINT count2)]
         * LogicalProject(inputs=[0], exprs=[[$TUMBLE($2, 5000:INTERVAL SECOND), $1, $3]]), rowType=[RecordType(INTEGER page_id, TIMESTAMP(3) *ROWTIME* $f1, INTEGER cnt, INTEGER cnt2)]
         * LogicalProject(inputs=[0..2], exprs=[[+($1, 10)]]), rowType=[RecordType(INTEGER page_id, INTEGER cnt, TIMESTAMP(3) *ROWTIME* ts, INTEGER cnt2)]
         * LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($2, 5000:INTERVAL SECOND)]), rowType=[RecordType(INTEGER page_id, INTEGER cnt, TIMESTAMP(3) *ROWTIME* ts)]
         * LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]]), rowType=[RecordType(INTEGER page_id, INTEGER cnt, TIMESTAMP(3) ts)]
         *
         * optimized plan digest:
         * Calc(select=[w$start AS window_start, w$end AS window_end, page_id, cnt, cnt2, count2], changelogMode=[I]), rowType=[RecordType(TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, INTEGER page_id, INTEGER cnt, INTEGER cnt2, BIGINT count2)]
         * GroupWindowAggregate(groupBy=[page_id], window=[TumblingGroupWindow('w$, ts, 5000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[page_id, SUM(cnt) AS cnt, SUM($f3) AS cnt2, COUNT(*) AS count2, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt, INTEGER cnt2, BIGINT count2, TIMESTAMP(3) w$start, TIMESTAMP(3) w$end, TIMESTAMP(3) *ROWTIME* w$rowtime, TIMESTAMP_LTZ(3) *PROCTIME* w$proctime)]
         * Exchange(distribution=[hash[page_id]], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, TIMESTAMP(3) *ROWTIME* ts, INTEGER cnt, INTEGER $f3)]
         * Calc(select=[page_id, ts, cnt, +(cnt, 10) AS $f3], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, TIMESTAMP(3) *ROWTIME* ts, INTEGER cnt, INTEGER $f3)]
         * WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt, TIMESTAMP(3) *ROWTIME* ts)]
         * TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt, ts], changelogMode=[I]), rowType=[RecordType(INTEGER page_id, INTEGER cnt, TIMESTAMP(3) ts)]
         */
        sql = """
              select
                TUMBLE_START(ts, INTERVAL '5' SECOND) as window_start,
                TUMBLE_END(ts, INTERVAL '5' SECOND) as window_end,
                page_id,
                sum(cnt) cnt,
                sum(cnt2) cnt2,
                count(1) count2
              from (
                select
                  *, cnt + 10 cnt2 
                from tmp_tb
              ) t
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

        env.execute("testBaseQuery");
    }

    @Test
    public void testTumbleEventTimeWindow2() throws Exception {
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
                  cnt int,
                  ts TIMESTAMP(3),
                  -- ts as CURRENT_TIMESTAMP,
                  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                ) WITH (
                  'connector'='datagen',
                  'rows-per-second'='1',
                  'fields.page_id.min'='1',
                  'fields.page_id.max'='2',
                  'fields.ts.max-past'='0',
                  'fields.cnt.min'='1',
                  'fields.cnt.max'='1'
                )
                """;
        tEnv.executeSql(sql);

        /**
         * 子查询计算无用的cnt2列直接被优化掉了
         * analyzed plan string:
         * LogicalProject(exprs=[[TUMBLE_START($1), TUMBLE_END($1), $0, $2, $3]])
         * +- LogicalAggregate(group=[{0, 1}], cnt=[SUM($2)], count2=[COUNT()])
         *    +- LogicalProject(inputs=[0], exprs=[[$TUMBLE($2, 5000:INTERVAL SECOND), $1]])
         *       +- LogicalProject(inputs=[0..2], exprs=[[+($1, 10)]])
         *          +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($2, 5000:INTERVAL SECOND)])
         *             +- LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]])
         *
         * optimized plan string:
         * Calc(select=[w$start AS window_start, w$end AS window_end, page_id, cnt, count2], upsertKeys=[[window_start, page_id], [window_end, page_id]])
         * +- GroupWindowAggregate(groupBy=[page_id], window=[TumblingGroupWindow('w$, ts, 5000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[page_id, SUM(cnt) AS cnt, COUNT(*) AS count2, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime], upsertKeys=[[page_id, w$proctime], [page_id, w$rowtime], [page_id, w$start], [page_id, w$end]])
         *    +- Exchange(distribution=[hash[page_id]])
         *       +- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)])
         *          +- TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt, ts])
         */
        sql = """
              select
                TUMBLE_START(ts, INTERVAL '5' SECOND) as window_start,
                TUMBLE_END(ts, INTERVAL '5' SECOND) as window_end,
                page_id,
                sum(cnt) cnt,
                count(1) count2
              from (
                select
                  *, cnt + 10 cnt2 
                from tmp_tb
              ) t
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

        env.execute("testBaseQuery");
    }

    @Test
    public void testTumbleEventTimeWindow3() throws Exception {
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
                  cnt int,
                  ts TIMESTAMP(3),
                  -- ts as CURRENT_TIMESTAMP,
                  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                ) WITH (
                  'connector'='datagen',
                  'rows-per-second'='1',
                  'fields.page_id.min'='1',
                  'fields.page_id.max'='2',
                  'fields.ts.max-past'='0',
                  'fields.cnt.min'='1',
                  'fields.cnt.max'='1'
                )
                """;
        tEnv.executeSql(sql);

        /**
         * analyzed plan string:
         * LogicalProject(exprs=[[TUMBLE_START($1), TUMBLE_END($1), $0, $2, $3]])
         * +- LogicalAggregate(group=[{0, 1}], cnt=[SUM($2)], count2=[COUNT()])
         *    +- LogicalProject(exprs=[[+($0, 10), $TUMBLE($2, 5000:INTERVAL SECOND), $1]])
         *       +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($2, 5000:INTERVAL SECOND)])
         *          +- LogicalTableScan(table=[[default_catalog, default_database, tmp_tb]])
         *
         * optimized plan string:
         * Calc(select=[w$start AS window_start, w$end AS window_end, $f0 AS page_id, cnt, count2], upsertKeys=[[window_start, page_id], [window_end, page_id]])
         * +- GroupWindowAggregate(groupBy=[$f0], window=[TumblingGroupWindow('w$, ts, 5000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[$f0, SUM(cnt) AS cnt, COUNT(*) AS count2, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime], upsertKeys=[[$f0, w$end], [$f0, w$rowtime], [$f0, w$start], [$f0, w$proctime]])
         *    +- Exchange(distribution=[hash[$f0]])
         *       +- Calc(select=[+(page_id, 10) AS $f0, ts, cnt])
         *          +- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 5000:INTERVAL SECOND)])
         *             +- TableSourceScan(table=[[default_catalog, default_database, tmp_tb]], fields=[page_id, cnt, ts])
         */
        sql = """
              select
                TUMBLE_START(ts, INTERVAL '5' SECOND) as window_start,
                TUMBLE_END(ts, INTERVAL '5' SECOND) as window_end,
                page_id + 10 page_id,
                sum(cnt) cnt,
                count(1) count2
              from tmp_tb
              group by page_id + 10, TUMBLE(ts, INTERVAL '5' SECOND)
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

        env.execute("testBaseQuery");
    }

}
