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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link SlidingProcessingTimeWindows}. */
class SlidingProcessingTimeWindowsTest {

    @Test
    void testWindowAssignment() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        SlidingProcessingTimeWindows assigner =
                SlidingProcessingTimeWindows.of(Time.milliseconds(5000), Time.milliseconds(1000));

        when(mockContext.getCurrentProcessingTime()).thenReturn(0L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(-4000, 1000),
                        new TimeWindow(-3000, 2000),
                        new TimeWindow(-2000, 3000),
                        new TimeWindow(-1000, 4000),
                        new TimeWindow(0, 5000));

        when(mockContext.getCurrentProcessingTime()).thenReturn(4999L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(0, 5000),
                        new TimeWindow(1000, 6000),
                        new TimeWindow(2000, 7000),
                        new TimeWindow(3000, 8000),
                        new TimeWindow(4000, 9000));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5000L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(1000, 6000),
                        new TimeWindow(2000, 7000),
                        new TimeWindow(3000, 8000),
                        new TimeWindow(4000, 9000),
                        new TimeWindow(5000, 10000));
    }

    @Test
    void testWindowAssignmentWithOffset() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        SlidingProcessingTimeWindows assigner =
                SlidingProcessingTimeWindows.of(
                        Time.milliseconds(5000), Time.milliseconds(1000), Time.milliseconds(100));

        when(mockContext.getCurrentProcessingTime()).thenReturn(100L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(-3900, 1100),
                        new TimeWindow(-2900, 2100),
                        new TimeWindow(-1900, 3100),
                        new TimeWindow(-900, 4100),
                        new TimeWindow(100, 5100));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5099L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(100, 5100),
                        new TimeWindow(1100, 6100),
                        new TimeWindow(2100, 7100),
                        new TimeWindow(3100, 8100),
                        new TimeWindow(4100, 9100));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5100L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(1100, 6100),
                        new TimeWindow(2100, 7100),
                        new TimeWindow(3100, 8100),
                        new TimeWindow(4100, 9100),
                        new TimeWindow(5100, 10100));
    }

    @Test
    void testWindowAssignmentWithNegativeOffset() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        SlidingProcessingTimeWindows assigner =
                SlidingProcessingTimeWindows.of(
                        Time.milliseconds(5000), Time.milliseconds(1000), Time.milliseconds(-100));

        when(mockContext.getCurrentProcessingTime()).thenReturn(0L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(-4100, 900),
                        new TimeWindow(-3100, 1900),
                        new TimeWindow(-2100, 2900),
                        new TimeWindow(-1100, 3900),
                        new TimeWindow(-100, 4900));

        when(mockContext.getCurrentProcessingTime()).thenReturn(4899L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(-100, 4900),
                        new TimeWindow(900, 5900),
                        new TimeWindow(1900, 6900),
                        new TimeWindow(2900, 7900),
                        new TimeWindow(3900, 8900));

        when(mockContext.getCurrentProcessingTime()).thenReturn(4900L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(900, 5900),
                        new TimeWindow(1900, 6900),
                        new TimeWindow(2900, 7900),
                        new TimeWindow(3900, 8900),
                        new TimeWindow(4900, 9900));
    }

    @Test
    void testTimeUnits() {
        // sanity check with one other time unit

        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        SlidingProcessingTimeWindows assigner =
                SlidingProcessingTimeWindows.of(
                        Time.seconds(5), Time.seconds(1), Time.milliseconds(500));

        when(mockContext.getCurrentProcessingTime()).thenReturn(100L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(-4500, 500),
                        new TimeWindow(-3500, 1500),
                        new TimeWindow(-2500, 2500),
                        new TimeWindow(-1500, 3500),
                        new TimeWindow(-500, 4500));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5499L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(500, 5500),
                        new TimeWindow(1500, 6500),
                        new TimeWindow(2500, 7500),
                        new TimeWindow(3500, 8500),
                        new TimeWindow(4500, 9500));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5100L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactlyInAnyOrder(
                        new TimeWindow(500, 5500),
                        new TimeWindow(1500, 6500),
                        new TimeWindow(2500, 7500),
                        new TimeWindow(3500, 8500),
                        new TimeWindow(4500, 9500));
    }

    @Test
    void testInvalidParameters() {

        assertThatThrownBy(() -> SlidingProcessingTimeWindows.of(Time.seconds(-2), Time.seconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < slide and size > 0");

        assertThatThrownBy(() -> SlidingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < slide and size > 0");

        assertThatThrownBy(
                        () ->
                                SlidingProcessingTimeWindows.of(
                                        Time.seconds(-20), Time.seconds(10), Time.seconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < slide and size > 0");

        assertThatThrownBy(
                        () ->
                                SlidingProcessingTimeWindows.of(
                                        Time.seconds(20), Time.seconds(10), Time.seconds(-11)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < slide and size > 0");

        assertThatThrownBy(
                        () ->
                                SlidingProcessingTimeWindows.of(
                                        Time.seconds(20), Time.seconds(10), Time.seconds(11)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < slide and size > 0");
    }

    @Test
    void testProperties() {
        SlidingProcessingTimeWindows assigner =
                SlidingProcessingTimeWindows.of(Time.seconds(5), Time.milliseconds(100));

        assertThat(assigner.isEventTime()).isFalse();
        assertThat(assigner.getWindowSerializer(new ExecutionConfig()))
                .isEqualTo(new TimeWindow.Serializer());
        assertThat(assigner.getDefaultTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
    }
}
