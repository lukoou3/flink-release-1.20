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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Not thread safe class for filling in the content of the {@link MemorySegment}. To access written
 * data please use {@link BufferConsumer} which allows to build {@link Buffer} instances from the
 * written data.
 */
@NotThreadSafe
public class BufferBuilder implements AutoCloseable {
    private final Buffer buffer;
    private final MemorySegment memorySegment;
    private int maxCapacity;

    private final SettablePositionMarker positionMarker = new SettablePositionMarker();

    private boolean bufferConsumerCreated = false;

    public BufferBuilder(MemorySegment memorySegment, BufferRecycler recycler) {
        this.memorySegment = checkNotNull(memorySegment);
        this.buffer = new NetworkBuffer(memorySegment, recycler);
        this.maxCapacity = buffer.getMaxCapacity();
    }

    /**
     * This method always creates a {@link BufferConsumer} starting from the current writer offset.
     * Data written to {@link BufferBuilder} before creation of {@link BufferConsumer} won't be
     * visible for that {@link BufferConsumer}.
     *
     * @return created matching instance of {@link BufferConsumer} to this {@link BufferBuilder}.
     */
    public BufferConsumer createBufferConsumer() {
        return createBufferConsumer(positionMarker.cachedPosition);
    }

    /**
     * This method always creates a {@link BufferConsumer} starting from position 0 of {@link
     * MemorySegment}.
     *
     * @return created matching instance of {@link BufferConsumer} to this {@link BufferBuilder}.
     */
    public BufferConsumer createBufferConsumerFromBeginning() {
        return createBufferConsumer(0);
    }

    private BufferConsumer createBufferConsumer(int currentReaderPosition) {
        checkState(
                !bufferConsumerCreated, "Two BufferConsumer shouldn't exist for one BufferBuilder");
        bufferConsumerCreated = true;
        // buffer.retainBuffer()增加引用, positionMarker传入
        return new BufferConsumer(buffer.retainBuffer(), positionMarker, currentReaderPosition);
    }

    /** Gets the data type of the internal buffer. */
    public Buffer.DataType getDataType() {
        return buffer.getDataType();
    }

    /** Sets the data type of the internal buffer. */
    public void setDataType(Buffer.DataType dataType) {
        buffer.setDataType(dataType);
    }

    /** Same as {@link #append(ByteBuffer)} but additionally {@link #commit()} the appending. */
    public int appendAndCommit(ByteBuffer source) {
        int writtenBytes = append(source);
        commit();
        return writtenBytes;
    }

    /**
     * Append as many data as possible from {@code source}. Not everything might be copied if there
     * is not enough space in the underlying {@link MemorySegment}
     *
     * @return number of copied bytes
     */
    public int append(ByteBuffer source) {
        checkState(!isFinished());

        int needed = source.remaining();
        int available = getMaxCapacity() - positionMarker.getCached();
        int toCopy = Math.min(needed, available);

        // 使用的positionMarker标记位置, 多次写入肯定有写完的时候
        memorySegment.put(positionMarker.getCached(), source, toCopy);
        positionMarker.move(toCopy);
        return toCopy;
    }

    /**
     * Make the change visible to the readers. This is costly operation (volatile access) thus in
     * case of bulk writes it's better to commit them all together instead one by one.
     */
    public void commit() {
        positionMarker.commit();
    }

    /**
     * Mark this {@link BufferBuilder} and associated {@link BufferConsumer} as finished - no new
     * data writes will be allowed.
     *
     * <p>This method should be idempotent to handle failures and task interruptions. Check
     * FLINK-8948 for more details.
     *
     * @return number of written bytes.
     */
    public int finish() {
        int writtenBytes = positionMarker.markFinished();
        commit();
        return writtenBytes;
    }

    public boolean isFinished() {
        return positionMarker.isFinished();
    }

    public boolean isFull() {
        checkState(positionMarker.getCached() <= getMaxCapacity());
        return positionMarker.getCached() == getMaxCapacity();
    }

    public int getWritableBytes() {
        checkState(positionMarker.getCached() <= getMaxCapacity());
        return getMaxCapacity() - positionMarker.getCached();
    }

    public int getCommittedBytes() {
        return positionMarker.getCached();
    }

    public int getMaxCapacity() {
        return maxCapacity;
    }

    /**
     * The result capacity can not be greater than allocated memorySegment. It also can not be less
     * than already written data.
     */
    public void trim(int newSize) {
        maxCapacity =
                Math.min(Math.max(newSize, positionMarker.getCached()), buffer.getMaxCapacity());
    }

    @Override
    public void close() {
        buffer.recycleBuffer();
    }

    /**
     * Holds a reference to the current writer position. Negative values indicate that writer
     * ({@link BufferBuilder} has finished. Value {@code Integer.MIN_VALUE} represents finished
     * empty buffer.
     */
    @ThreadSafe
    interface PositionMarker {
        int FINISHED_EMPTY = Integer.MIN_VALUE;

        int get();

        static boolean isFinished(int position) {
            return position < 0;
        }

        static int getAbsolute(int position) {
            if (position == FINISHED_EMPTY) {
                return 0;
            }
            return Math.abs(position);
        }
    }

    /**
     * PositionMarker的缓存写入实现。
     * 写入器（BufferBuilder）和读取器（BufferConsumer）缓存必须彼此独立实现，这样缓存的值就不会意外地从一个泄漏到另一个。
     * 记得提交BufferBuilder。SettablePositionMarker使更改可见。
     *
     * Cached writing implementation of {@link PositionMarker}.
     *
     * <p>Writer ({@link BufferBuilder}) and reader ({@link BufferConsumer}) caches must be
     * implemented independently of one another - so that the cached values can not accidentally
     * leak from one to another.
     *
     * <p>Remember to commit the {@link SettablePositionMarker} to make the changes visible.
     */
    static class SettablePositionMarker implements PositionMarker {
        private volatile int position = 0;

        /**
         * 本地缓存volatile位置的值，以避免不必要的volatile访问。
         * Locally cached value of volatile {@code position} to avoid unnecessary volatile accesses.
         */
        private int cachedPosition = 0;

        @Override
        public int get() {
            return position;
        }

        public boolean isFinished() {
            return PositionMarker.isFinished(cachedPosition);
        }

        public int getCached() {
            return PositionMarker.getAbsolute(cachedPosition);
        }

        /**
         * Marks this position as finished and returns the current position.
         *
         * @return current position as of {@link #getCached()}
         */
        public int markFinished() {
            int currentPosition = getCached();
            int newValue = -currentPosition;
            if (newValue == 0) {
                newValue = FINISHED_EMPTY;
            }
            set(newValue);
            return currentPosition;
        }

        public void move(int offset) {
            set(cachedPosition + offset);
        }

        public void set(int value) {
            cachedPosition = value;
        }

        public void commit() {
            position = cachedPosition;
        }
    }
}
