package com.test.stream;

import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.BoxedWrapperRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator$;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.DefaultCalciteContext;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.mutable.ArrayBuffer;


import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public class FlinkSqlParserTest {

    @Test
    public void test() throws Exception {
        StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl) createTableEnv();
        String sqlText = "select col1, substr(col2, 1, 10) b, col3 + 10 c, abs(col) from tbl";
        SqlNode sqlNode = createSqlParser(sqlText).parseStmt();
        System.out.println(sqlNode);
        StreamPlanner planner = (StreamPlanner) tEnv.getPlanner();
        DefaultCalciteContext defaultCalciteContext = new DefaultCalciteContext(planner.catalogManager(), planner.plannerContext());
        FlinkPlannerImpl flinkPlanner = defaultCalciteContext.getPlannerContext().createFlinkPlanner();
        var sql = """
                CREATE TEMPORARY TABLE tbl (
                  `col1` STRING,
                  `col2` STRING,
                  `col3` INT,
                  `col` INT
                ) WITH (
                  'connector' = 'datagen',
                  'rows-per-second' = '1'
                )
                """;
        tEnv.executeSql(sql);
        // validate the query
        final SqlNode validated = flinkPlanner.validate(sqlNode);
        System.out.println(validated);
        CatalogManager catalogManager = tEnv.getCatalogManager();
        Operation operation = SqlNodeToOperationConversion.convert(flinkPlanner, catalogManager, validated).get();
        System.out.println(operation);
        PlannerQueryOperation queryOperation = (PlannerQueryOperation) operation;
        LogicalProject project = (LogicalProject) queryOperation.getCalciteTree();
        System.out.println(project);
        ResolvedSchema resolvedSchema = queryOperation.getResolvedSchema();
        RowType outType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
        CodeGeneratorContext ctx = new CodeGeneratorContext(TableConfig.getDefault(), Thread.currentThread().getContextClassLoader());
        RowType inputType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("col1", DataTypes.STRING()), DataTypes.FIELD("col2", DataTypes.STRING()),
                DataTypes.FIELD("col3", DataTypes.INT()), DataTypes.FIELD("col", DataTypes.INT())
        ).getLogicalType();
        InternalTypeInfo<RowData> inputTypeInfo = InternalTypeInfo.of(inputType);
        ArrayBuffer projection = new ArrayBuffer();
        for (RexNode projectProject : project.getProjects()) {
            projection.$plus$eq(projectProject);
        }
        // 代码生成
        CalcCodeGenerator.generateCalcOperator(ctx, new OneInputTransformation<RowData,RowData>(null, "", ( OneInputStreamOperator<RowData,RowData>) null, inputTypeInfo, 1)
                , outType, projection,
                scala.Option.empty(), true, "StreamExecCalc");
        GeneratedFunction<FlatMapFunction<RowData, RowData>> function = CalcCodeGenerator$.MODULE$.generateFunction(inputType, "Function", outType, BoxedWrapperRowData.class,
                projection, Option.empty(), TableConfig.getDefault(), Thread.currentThread().getContextClassLoader());
        System.out.println(function.getCode());
    }

    @Test
    public void testSelectWithoutFrom() throws Exception {
        String[] sqlTexts = new String[] {
                "select * from tbl",
                "select col1, substr(col2, 1, 10) b, col3 + 10 c, abs(col) from tbl",
                "select *",
                "select col1, substr(col2, 1, 10) b, col3 + 10 c, abs(col)",
        };
        for (String sqlText : sqlTexts) {
            SqlNode sqlNode = createSqlParser(sqlText).parseStmt();
            System.out.println(sqlNode);
            System.out.println("----");
        }
    }

    @Test
    public void testFilter() throws Exception {
        StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl) createTableEnv();
        String sqlText = "select * from tbl where col3 > 30";
        SqlNode sqlNode = createSqlParser(sqlText).parseStmt();
        System.out.println(sqlNode);
        StreamPlanner planner = (StreamPlanner) tEnv.getPlanner();
        DefaultCalciteContext defaultCalciteContext = new DefaultCalciteContext(planner.catalogManager(), planner.plannerContext());
        FlinkPlannerImpl flinkPlanner = defaultCalciteContext.getPlannerContext().createFlinkPlanner();
        var sql = """
                CREATE TEMPORARY TABLE tbl (
                  `col1` STRING,
                  `col2` STRING,
                  `col3` INT,
                  `col` INT
                ) WITH (
                  'connector' = 'datagen',
                  'rows-per-second' = '1'
                )
                """;
        tEnv.executeSql(sql);
        // validate the query
        final SqlNode validated = flinkPlanner.validate(sqlNode);
        System.out.println(validated);
        CatalogManager catalogManager = tEnv.getCatalogManager();
        Operation operation = SqlNodeToOperationConversion.convert(flinkPlanner, catalogManager, validated).get();
        System.out.println(operation);
        PlannerQueryOperation queryOperation = (PlannerQueryOperation) operation;
        LogicalProject project = (LogicalProject) queryOperation.getCalciteTree();
        LogicalFilter filter = (LogicalFilter) project.getInput();
        RexNode condition = filter.getCondition();
        System.out.println(project);
        System.out.println(filter);
        System.out.println(condition);
        ResolvedSchema resolvedSchema = queryOperation.getResolvedSchema();
        RowType outType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
        CodeGeneratorContext ctx = new CodeGeneratorContext(TableConfig.getDefault(), Thread.currentThread().getContextClassLoader());
        RowType inputType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("col1", DataTypes.STRING()), DataTypes.FIELD("col2", DataTypes.STRING()),
                DataTypes.FIELD("col3", DataTypes.INT()), DataTypes.FIELD("col", DataTypes.INT())
        ).getLogicalType();
        InternalTypeInfo<RowData> inputTypeInfo = InternalTypeInfo.of(inputType);
        ArrayBuffer projection = new ArrayBuffer();
        for (RexNode projectProject : project.getProjects()) {
            projection.$plus$eq(projectProject);
        }
        // 代码生成
        CalcCodeGenerator.generateCalcOperator(ctx, new OneInputTransformation<RowData,RowData>(null, "", ( OneInputStreamOperator<RowData,RowData>) null, inputTypeInfo, 1)
                , outType, projection,
                scala.Option.apply(condition), true, "StreamExecCalc");
        GeneratedFunction<FlatMapFunction<RowData, RowData>> function = CalcCodeGenerator$.MODULE$.generateFunction(inputType, "Function", outType, BoxedWrapperRowData.class,
                projection, scala.Option.apply(condition), TableConfig.getDefault(), Thread.currentThread().getContextClassLoader());
        System.out.println(function.getCode());
    }

    @Test
    public void testDataTypes() throws Exception {
        StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl) createTableEnv();
        CatalogManager catalogManager = tEnv.getCatalogManager();
        DataTypeFactory dataTypeFactory = catalogManager.getDataTypeFactory();
        String[] types = new String[] {"INT", "BIGINT", "STRING", "ARRAY<INT>", "ROW<a INT, b STRING>"};
        for (String type : types) {
            DataType dataType = dataTypeFactory.createDataType(type);
            System.out.println(type + ":");
            System.out.println(dataType);
            System.out.println(dataType.getLogicalType());
            System.out.println(dataTypeFactory.createLogicalType(type));
        }
    }

    @Test
    public void testSqlParser() throws Exception {
        String sqlText = "select col1, substr(col2, 1, 10) b, col3 + 10 c, func(col, 4) from tbl";
        SqlNode sqlNode = createSqlParser(sqlText).parseStmt();
        System.out.println(sqlNode);

        String[] expressions = new String[] {"col1", "substr(col2, 1, 10)", "col3 + 10", "func(col, 4)"};
        for (String expression : expressions) {
            // parseExpression 没法定义别名
            SqlNode expr = createSqlParser(expression).parseExpression();
            System.out.println(expr);
        }
    }

    // calcite 原生的sql解析
    private SqlParser createSqlParser(String sql) {
        SqlParser parser = SqlParser.create(sql, getSqlParserConfig());
        return parser;
    }

    // flink sql 包装的sql解析, 就只是加了异常处理
    private CalciteParser createCalciteParser() {
        return new CalciteParser(getSqlParserConfig());
    }

    // org.apache.flink.table.planner.delegation.PlannerContext.getSqlParserConfig
    private SqlParser.Config getSqlParserConfig() {
        SqlConformance conformance = FlinkSqlConformance.DEFAULT;
        return SqlParser.config()
                .withParserFactory(FlinkSqlParserFactories.create(conformance))
                .withConformance(conformance)
                .withLex(Lex.JAVA)
                .withIdentifierMaxLength(256);
    }

    private StreamTableEnvironment createTableEnv() {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        return tEnv;
    }

}

