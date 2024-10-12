/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.InputFormatOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link LegacySourceTransformation}.
 *
 * @param <OUT> The type of the elements that the {@link LegacySourceTransformation} we are
 *     translating is producing.
 */
@Internal
public class LegacySourceTransformationTranslator<OUT>
        extends SimpleTransformationTranslator<OUT, LegacySourceTransformation<OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final LegacySourceTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final LegacySourceTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    private Collection<Integer> translateInternal(
            final LegacySourceTransformation<OUT> transformation, final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        final StreamGraph streamGraph = context.getStreamGraph();
        final String slotSharingGroup = context.getSlotSharingGroup();
        // transformationId是自动生成的, 自增
        final int transformationId = transformation.getId();
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

        /**
         * streamNode放入streamNodes, vertexID加入streamGraph的sources
         * 和OneInputTransformationTranslator一样也是调用的addOperator方法
         * Source和其它的节点不一样的是不需要调用addEdge的操作, source是首个节点不需要添加边, 转换其它的节点时添加inputNode->thisNode边即可
         */
        streamGraph.addLegacySource(
                transformationId,
                slotSharingGroup,
                transformation.getCoLocationGroupKey(),
                transformation.getOperatorFactory(),
                null,
                transformation.getOutputType(),
                "Source: " + transformation.getName());

        if (transformation.getOperatorFactory() instanceof InputFormatOperatorFactory) {
            streamGraph.setInputFormat(
                    transformationId,
                    ((InputFormatOperatorFactory<OUT>) transformation.getOperatorFactory())
                            .getInputFormat());
        }

        final int parallelism =
                transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
                        ? transformation.getParallelism()
                        : executionConfig.getParallelism();
        streamGraph.setParallelism(
                transformationId, parallelism, transformation.isParallelismConfigured());
        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

        streamGraph.setSupportsConcurrentExecutionAttempts(
                transformationId, transformation.isSupportsConcurrentExecutionAttempts());

        return Collections.singleton(transformationId);
    }
}
