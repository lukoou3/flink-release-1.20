package org.apache.flink.table.planner.delegation;

import org.apache.calcite.rel.RelNode;

import org.apache.calcite.sql.SqlExplainLevel;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;

import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;

import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;

import java.util.List;

public class PlannerHelper {

    public static Transformation<RowData> translate(AbstractStreamTableEnvironmentImpl tEnv, QueryOperation queryOperation) {
        return translate(tEnv, queryOperation, true);
    }

    // QueryOperation(RelNode)优化转为Transformation
    public static Transformation<RowData> translate(AbstractStreamTableEnvironmentImpl tEnv, QueryOperation queryOperation, boolean printPlan) {
        String analyzedString = null, analyzedDigest = null, optimizedString = null, optimizedDigest = null;
        StreamPlanner planner = (StreamPlanner) tEnv.getPlanner();
        planner.beforeTranslation();
        RelNode relNode = planner.createRelBuilder().queryOperation(queryOperation).build();
        if (printPlan) {
            analyzedString = relNodeToString(relNode);
            analyzedDigest = relNodeDigest(relNode);
        }
        ArrayBuffer<RelNode> relNodes = new ArrayBuffer();
        relNodes.$plus$eq(relNode);
        Seq<RelNode> optimizedRelNodes = planner.optimize(relNodes);
        if (printPlan) {
            optimizedString = relNodeToString(optimizedRelNodes.apply(0));
            optimizedDigest = relNodeDigest(optimizedRelNodes.apply(0));
            System.out.println("analyzed plan string:");
            System.out.println(analyzedString);
            System.out.println("optimized plan string:");
            System.out.println(optimizedString);
            System.out.println("analyzed plan digest:");
            System.out.println(analyzedDigest);
            System.out.println("optimized plan digest:");
            System.out.println(optimizedDigest);
        }
        ExecNodeGraph execGraph = planner.translateToExecNodeGraph(optimizedRelNodes, false);
        List<Transformation<?>> transformations = planner.translateToPlan(execGraph);
        planner.afterTranslation();
        if (transformations.size() != 1) {
            throw new TableException(String.format( "Expected a single transformation for query: %s\n Got: %s", queryOperation.asSummaryString(), transformations));
        }
        final Transformation<RowData> transformation = (Transformation<RowData>) transformations.get(0);
        return transformation;
    }

    public static String relNodeToString(RelNode relNode) {
        return relNodeToString(
                relNode,
                SqlExplainLevel.DIGEST_ATTRIBUTES,
                false,
                false,
                false,
                true,
                false);
    }

    public static String relNodeToString(RelNode relNode, SqlExplainLevel detailLevel, boolean withIdPrefix, boolean withChangelogTraits,
                                         boolean withRowType, boolean withUpsertKey, boolean withQueryBlockAlias) {
        return FlinkRelOptUtil.toString(relNode, detailLevel, withIdPrefix, withChangelogTraits, withRowType, withUpsertKey, withQueryBlockAlias);
    }

    public static String relNodeDigest(RelNode relNode) {
        return FlinkRelOptUtil.getDigest(relNode);
    }
}
