/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A utility base class for one input {@link Transformation transformations} that provides a
 * function for configuring common graph properties.
 */
abstract class AbstractOneInputTransformationTranslator<IN, OUT, OP extends Transformation<OUT>>
        extends SimpleTransformationTranslator<OUT, OP> {

    protected Collection<Integer> translateInternal(
            final Transformation<OUT> transformation,
            final StreamOperatorFactory<OUT> operatorFactory,
            final TypeInformation<IN> inputType,
            @Nullable final KeySelector<IN, ?> stateKeySelector,
            @Nullable final TypeInformation<?> stateKeyType,
            final Context context) {
        checkNotNull(transformation);
        checkNotNull(operatorFactory);
        checkNotNull(inputType);
        checkNotNull(context);

        final StreamGraph streamGraph = context.getStreamGraph();
        final String slotSharingGroup = context.getSlotSharingGroup();
        // transformationId是自动生成的, 自增
        final int transformationId = transformation.getId();
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

        /**
         * 主要就是这个方法, streamNode放入streamNodes
         */
        streamGraph.addOperator(
                transformationId,
                slotSharingGroup,
                transformation.getCoLocationGroupKey(),
                operatorFactory,
                inputType,
                transformation.getOutputType(),
                transformation.getName());

        if (stateKeySelector != null) {
            TypeSerializer<?> keySerializer =
                    stateKeyType.createSerializer(executionConfig.getSerializerConfig());
            streamGraph.setOneInputStateKey(transformationId, stateKeySelector, keySerializer);
        }

        int parallelism =
                transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
                        ? transformation.getParallelism()
                        : executionConfig.getParallelism();
        streamGraph.setParallelism(
                transformationId, parallelism, transformation.isParallelismConfigured());
        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

        final List<Transformation<?>> parentTransformations = transformation.getInputs();
        checkState(
                parentTransformations.size() == 1,
                "Expected exactly one input transformation but found "
                        + parentTransformations.size());

        /**
         * 添加Edge, 连接相邻的Operator/streamNode
         * 每个StreamNode都有inEdges, outEdges, source类型的StreamNode的inEdges为null, sink类型的StreamNode的outEdges为null
         */
        for (Integer inputId : context.getStreamNodeIds(parentTransformations.get(0))) {
            streamGraph.addEdge(inputId, transformationId, 0);
        }

        if (transformation instanceof PhysicalTransformation) {
            streamGraph.setSupportsConcurrentExecutionAttempts(
                    transformationId,
                    ((PhysicalTransformation<OUT>) transformation)
                            .isSupportsConcurrentExecutionAttempts());
        }

        return Collections.singleton(transformationId);
    }
}
