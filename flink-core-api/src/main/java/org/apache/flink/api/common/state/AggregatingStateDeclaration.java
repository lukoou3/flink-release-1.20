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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeDescriptor;

/** This represents a declaration of the aggregating state. */
@Experimental
public interface AggregatingStateDeclaration<IN, ACC, OUT> extends StateDeclaration {
    /** Get type descriptor of this state. */
    TypeDescriptor<ACC> getTypeDescriptor();

    /** Get the aggregate function of this state. */
    AggregateFunction<IN, ACC, OUT> getAggregateFunction();
}
