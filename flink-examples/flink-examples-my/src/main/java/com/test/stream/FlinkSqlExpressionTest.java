package com.test.stream;

import org.apache.calcite.rex.RexNode;

import org.apache.calcite.sql.SqlNode;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.SqlToRexConverter;
import org.apache.flink.table.planner.delegation.StreamPlanner;

import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public class FlinkSqlExpressionTest {

    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        CatalogManager catalogManager = ((StreamTableEnvironmentImpl)tEnv).getCatalogManager();
        DataTypeFactory dataTypeFactory = catalogManager.getDataTypeFactory();
        StreamPlanner planner = (StreamPlanner) ((AbstractStreamTableEnvironmentImpl)tEnv).getPlanner();
        FlinkTypeFactory typeFactory = planner.getTypeFactory();
        FlinkPlannerImpl flinkPlanner = planner.plannerContext().createFlinkPlanner();
        CalciteParser calciteParser = planner.plannerContext().createCalciteParser();
        RowType rowType = (RowType)dataTypeFactory.createDataType("ROW<name string, cnt int>").getLogicalType();
        SqlToRexConverter sqlToRexConverter = planner.getFlinkContext().getRexFactory()
                .createSqlToRexConverter(rowType, null);
        String[] exprs = new String[]{"name", "cnt", "substr(name, 1, 4)", "cnt + 100" };
        for (String expr : exprs) {
            RexNode rexNode = sqlToRexConverter.convertToRexNode(expr);
            RexNode rexNode2 = flinkPlanner.rex(
                    calciteParser.parseExpression(expr),
                    typeFactory.buildRelNodeRowType(rowType),
                    null);
            System.out.println(expr);
            System.out.println(rexNode);
            System.out.println(rexNode2);
            System.out.println(rexNode.equals(rexNode2));
            final LogicalType logicalType = FlinkTypeFactory.toLogicalType(rexNode.getType());
            DataType dataType = TypeConversions.fromLogicalToDataType(logicalType);
            System.out.println(logicalType);
            System.out.println(dataType);
        }

        SqlNode sqlNode = calciteParser.parse("select * from tmp");
        System.out.println(sqlNode);
    }

}
