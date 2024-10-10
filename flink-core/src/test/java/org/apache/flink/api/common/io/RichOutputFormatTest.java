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

package org.apache.flink.api.common.io;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.types.Value;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests runtime context access from inside an RichOutputFormat class. */
class RichOutputFormatTest {

    @Test
    void testCheckRuntimeContextAccess() {
        final SerializedOutputFormat<Value> inputFormat = new SerializedOutputFormat<>();
        final TaskInfo taskInfo = new TaskInfoImpl("test name", 3, 1, 3, 0);

        inputFormat.setRuntimeContext(
                new RuntimeUDFContext(
                        taskInfo,
                        getClass().getClassLoader(),
                        new ExecutionConfig(),
                        new HashMap<>(),
                        new HashMap<>(),
                        UnregisteredMetricsGroup.createOperatorMetricGroup()));

        assertThat(inputFormat.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()).isOne();
        assertThat(inputFormat.getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks())
                .isEqualTo(3);
    }
}