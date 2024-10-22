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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 有界的event-time窗口聚合函数。
 * sum(c) OVER (PARTITION BY b ORDER BY rowtime RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW)
 *
 * 基本和proc-time一样，就是这里有过期数据丢弃的处理。
 * 每行的数据当watermark到此行数据的时间就会输出
 * 不过这里有过期数据丢弃的处理，当元素到来时如果元素时间小于等于上一次触发的时间就会丢弃，
 * 也是正常情况下如果元素时间小于当前的watermark就会被丢弃，
 * 这样的处理逻辑还是可以的，只要设置合适的watermark，基本就没有丢弃的数据，丢弃的数据可以在Metric看到(name = numLateRecordsDropped)
 *
 * Process Function for RANGE clause event-time bounded OVER window.
 *
 * <p>E.g.: SELECT rowtime, b, c, min(c) OVER (PARTITION BY b ORDER BY rowtime RANGE BETWEEN
 * INTERVAL '4' SECOND PRECEDING AND CURRENT ROW), max(c) OVER (PARTITION BY b ORDER BY rowtime
 * RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW) FROM T.
 */
public class RowTimeRangeBoundedPrecedingFunction<K>
        extends KeyedProcessFunction<K, RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(RowTimeRangeBoundedPrecedingFunction.class);

    private final GeneratedAggsHandleFunction genAggsHandler;
    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final long precedingOffset;
    private final int rowTimeIdx;

    private transient JoinedRowData output;

    // the state which keeps the last triggering timestamp
    private transient ValueState<Long> lastTriggeringTsState;

    // the state which used to materialize the accumulator for incremental calculation
    private transient ValueState<RowData> accState;

    // the state which keeps the safe timestamp to cleanup states
    private transient ValueState<Long> cleanupTsState;

    // the state which keeps all the data that are not expired.
    // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
    // the second element of tuple is a list that contains the entire data of all the rows belonging
    // to this time stamp.
    private transient MapState<Long, List<RowData>> inputState;

    private transient AggsHandleFunction function;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private transient Counter numLateRecordsDropped;

    @VisibleForTesting
    protected Counter getCounter() {
        return numLateRecordsDropped;
    }

    public RowTimeRangeBoundedPrecedingFunction(
            GeneratedAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            long precedingOffset,
            int rowTimeIdx) {
        Preconditions.checkNotNull(precedingOffset);
        this.genAggsHandler = genAggsHandler;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        // 窗口的时长
        this.precedingOffset = precedingOffset;
        this.rowTimeIdx = rowTimeIdx;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        output = new JoinedRowData();

        ValueStateDescriptor<Long> lastTriggeringTsDescriptor =
                new ValueStateDescriptor<Long>("lastTriggeringTsState", Types.LONG);
        lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accStateDesc =
                new ValueStateDescriptor<RowData>("accState", accTypeInfo);
        accState = getRuntimeContext().getState(accStateDesc);

        // input element are all binary row as they are came from network
        InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(inputFieldTypes);
        ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<RowData>(inputType);
        MapStateDescriptor<Long, List<RowData>> inputStateDesc =
                new MapStateDescriptor<Long, List<RowData>>(
                        "inputState", Types.LONG, rowListTypeInfo);
        inputState = getRuntimeContext().getMapState(inputStateDesc);

        ValueStateDescriptor<Long> cleanupTsStateDescriptor =
                new ValueStateDescriptor<>("cleanupTsState", Types.LONG);
        this.cleanupTsState = getRuntimeContext().getState(cleanupTsStateDescriptor);

        // metrics
        this.numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    /**
     *
     * 想实现多个窗口的聚合，可不可以使用最大的窗口，使用if过滤？
     * 看了下源码，并不能，input每次就确定了要是输入curren_timestamp,元素输入时就会计算好，而不是over聚合时再计算
     * 而且聚合有增量加减的逻辑，也不应该你有这种实现
     */
    @Override
    public void processElement(
            RowData input,
            KeyedProcessFunction<K, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws Exception {
        // 触发时间, 当watermark到此行数据的时间就会输出
        // triggering timestamp for trigger calculation
        long triggeringTs = input.getLong(rowTimeIdx);

        // 上一次触发的ts
        Long lastTriggeringTs = lastTriggeringTsState.value();
        if (lastTriggeringTs == null) {
            lastTriggeringTs = 0L;
        }

        /**
         * 检查元素是否过期：
         *      没过期，保存元素并且注册定时器等到watermark到这个元素的时间，输出元素
         *      过期，直接丢弃，更新numLateRecordsDropped Metric
         */
        // check if the data is expired, if not, save the data and register event time timer
        if (triggeringTs > lastTriggeringTs) {
            List<RowData> data = inputState.get(triggeringTs);
            if (null != data) {
                data.add(input);
                inputState.put(triggeringTs, data);
            } else {
                data = new ArrayList<RowData>();
                data.add(input);
                inputState.put(triggeringTs, data);
                // register event time timer
                ctx.timerService().registerEventTimeTimer(triggeringTs);
            }
            registerCleanupTimer(ctx, triggeringTs);
        } else {
            numLateRecordsDropped.inc();
        }
    }

    private void registerCleanupTimer(
            KeyedProcessFunction<K, RowData, RowData>.Context ctx, long timestamp)
            throws Exception {
        // calculate safe timestamp to cleanup states
        long minCleanupTimestamp = timestamp + precedingOffset + 1;
        long maxCleanupTimestamp = timestamp + (long) (precedingOffset * 1.5) + 1;
        // update timestamp and register timer if needed
        Long curCleanupTimestamp = cleanupTsState.value();
        if (curCleanupTimestamp == null || curCleanupTimestamp < minCleanupTimestamp) {
            // we don't delete existing timer since it may delete timer for data processing
            // TODO Use timer with namespace to distinguish timers
            ctx.timerService().registerEventTimeTimer(maxCleanupTimestamp);
            cleanupTsState.update(maxCleanupTimestamp);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out)
            throws Exception {
        // 清理状态数据的代码，可以先忽略
        Long cleanupTimestamp = cleanupTsState.value();
        // if cleanupTsState has not been updated then it is safe to cleanup states
        if (cleanupTimestamp != null && cleanupTimestamp <= timestamp) {
            inputState.clear();
            accState.clear();
            lastTriggeringTsState.clear();
            cleanupTsState.clear();
            function.cleanup();
            return;
        }

        // 当前时间的元素list
        // gets all window data from state for the calculation
        List<RowData> inputs = inputState.get(timestamp);

        if (null != inputs) {

            int dataListIndex = 0;
            RowData accumulators = accState.value();

            // initialize when first run or failover recovery per key
            if (null == accumulators) {
                accumulators = function.createAccumulators();
            }
            // set accumulators in context first
            function.setAccumulators(accumulators);

            // keep up timestamps of retract data
            List<Long> retractTsList = new ArrayList<Long>();

            // do retraction
            Iterator<Map.Entry<Long, List<RowData>>> iter = inputState.entries().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, List<RowData>> data = iter.next();
                Long dataTs = data.getKey();
                Long offset = timestamp - dataTs;
                // 聚合值减去过期的数据
                if (offset > precedingOffset) {
                    List<RowData> retractDataList = data.getValue();
                    if (retractDataList != null) {
                        dataListIndex = 0;
                        while (dataListIndex < retractDataList.size()) {
                            RowData retractRow = retractDataList.get(dataListIndex);
                            function.retract(retractRow);
                            dataListIndex += 1;
                        }
                        retractTsList.add(dataTs);
                    } else {
                        // Does not retract values which are outside of window if the state is
                        // cleared already.
                        LOG.warn(
                                "The state is cleared because of state ttl. "
                                        + "This will result in incorrect result. "
                                        + "You can increase the state ttl to avoid this.");
                    }
                }
            }

            // 把当前时间戳的元素聚合数据
            // do accumulation
            dataListIndex = 0;
            while (dataListIndex < inputs.size()) {
                RowData curRow = inputs.get(dataListIndex);
                // accumulate current row
                function.accumulate(curRow);
                dataListIndex += 1;
            }

            // get aggregate result
            RowData aggValue = function.getValue();

            // 把当前时间戳的元素输出
            // copy forwarded fields to output row and emit output row
            dataListIndex = 0;
            while (dataListIndex < inputs.size()) {
                RowData curRow = inputs.get(dataListIndex);
                output.replace(curRow, aggValue);
                out.collect(output);
                dataListIndex += 1;
            }

            // 删除过期数据
            // remove the data that has been retracted
            dataListIndex = 0;
            while (dataListIndex < retractTsList.size()) {
                inputState.remove(retractTsList.get(dataListIndex));
                dataListIndex += 1;
            }

            /**
             * 更新聚合值, 可以看到这里流聚合的实现还是挺高效的，不是每次遍历全部，而是减去过期的加上新增的
             * 看了下自定义聚合函数，AggregateFunction的retract() 在 bounded OVER 窗口中是必须实现的
             */
            // update the value of accumulators for future incremental computation
            accumulators = function.getAccumulators();
            accState.update(accumulators);
        }
        lastTriggeringTsState.update(timestamp);
    }

    @Override
    public void close() throws Exception {
        if (null != function) {
            function.close();
        }
    }
}
