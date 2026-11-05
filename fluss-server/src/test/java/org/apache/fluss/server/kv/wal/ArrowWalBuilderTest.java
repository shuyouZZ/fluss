/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.wal;

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.compression.ArrowCompressionType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.arrow.ArrowWriterProvider;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEqualsWithRowKind;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ArrowWalBuilder}. */
class ArrowWalBuilderTest {
    private BufferAllocator allocator;
    private ArrowWriterProvider arrowWriterProvider;
    private Configuration conf;

    @BeforeEach
    void setUp() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.arrowWriterProvider = new ArrowWriterPool(allocator);
        this.conf = new Configuration();
    }

    @AfterEach
    void tearDown() throws Exception {
        arrowWriterProvider.close();
        allocator.close();
    }

    @Test
    void testArrowWalBuilderCrossSeveralMemoryPages() throws Exception {
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("2kb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("128b"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("128b"));

        int bucketId = 0;
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, bucketId);
        LazyMemorySegmentPool memorySegmentPool =
                LazyMemorySegmentPool.createWriterBufferPool(conf);
        WalBuilder walBuilder = createWalBuilder(tb, 1024, memorySegmentPool);

        List<Tuple2<ChangeType, Object[]>> expectedResult = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ChangeType changeType = ChangeType.INSERT;
            Object[] objects = {i, "v" + i};
            walBuilder.append(changeType, row(objects));
            expectedResult.add(Tuple2.of(changeType, objects));
        }

        // consume log records before walBuilder deallocate memory. it's safe.
        MemoryLogRecords logRecords = walBuilder.build();
        long totalPages = memorySegmentPool.totalSize() / memorySegmentPool.pageSize();
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        // allocate multiple pages
        assertThat(totalPages - memorySegmentPool.freePages()).isGreaterThan(1);
        assertLogRecordsEqualsWithRowKind(DATA1_ROW_TYPE, logRecords, expectedResult);

        // consume log records after walBuilder deallocate memory. Even the content in memory
        // segment pool is changed, the log records is still valid. Because the memory of logRecords
        // is deeply copied.
        walBuilder.deallocate();
        assertThat(memorySegmentPool.freePages()).isEqualTo(totalPages);
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        // change some bytes of memory segment pool.
        for (MemorySegment memorySegment : memorySegmentPool.getAllCachePages()) {
            memorySegment.put(50, (byte) 4);
        }
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        assertLogRecordsEqualsWithRowKind(DATA1_ROW_TYPE, logRecords, expectedResult);
    }

    @Test
    void testArrowWalBuilderWithinOneMemoryPage() throws Exception {
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("4kb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));

        int bucketId = 0;
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, bucketId);
        LazyMemorySegmentPool memorySegmentPool =
                LazyMemorySegmentPool.createWriterBufferPool(conf);
        WalBuilder walBuilder = createWalBuilder(tb, 1024, memorySegmentPool);

        List<Tuple2<ChangeType, Object[]>> expectedResult = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ChangeType changeType = ChangeType.INSERT;
            Object[] objects = {i, "v" + i};
            walBuilder.append(changeType, row(objects));
            expectedResult.add(Tuple2.of(changeType, objects));
        }

        // consume log records before walBuilder deallocate memory. it's safe.
        MemoryLogRecords logRecords = walBuilder.build();
        long totalPages = memorySegmentPool.totalSize() / memorySegmentPool.pageSize();
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        // allocate one page
        assertThat(totalPages - memorySegmentPool.freePages()).isEqualTo(1);
        assertLogRecordsEqualsWithRowKind(DATA1_ROW_TYPE, logRecords, expectedResult);

        // consume log records after walBuilder deallocate memory. While the content in memory
        // segment pool is changed, the log records will be invalid. Because the memory of
        // logRecords use the same byteBuffer with the segment in memory segment pool.
        walBuilder.deallocate();
        assertThat(memorySegmentPool.freePages()).isEqualTo(totalPages);
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        // change some bytes of memory segment pool.
        for (MemorySegment memorySegment : memorySegmentPool.getAllCachePages()) {
            memorySegment.put(50, (byte) 4);
        }
        assertThat(logRecords.batches().iterator().next().isValid()).isFalse();
    }

    @Test
    void testSerializedSizesIncludeActualIpcMetadataAndEncodedBody() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 0);
        LazyMemorySegmentPool pool = LazyMemorySegmentPool.createWriterBufferPool(conf);
        ArrowWalBuilder builder = (ArrowWalBuilder) createWalBuilder(tb, 1024, pool);
        try {
            builder.append(ChangeType.INSERT, row(1, "initial value"));
            builder.append(ChangeType.UPDATE_BEFORE, row(1, "initial value"));
            builder.append(ChangeType.UPDATE_AFTER, row(1, "updated value"));
            MemoryLogRecords records = builder.build();
            LogRecordBatch batch = records.batches().iterator().next();
            int headerBytes = recordBatchHeaderSize(batch.magic());
            int arrowOffset = headerBytes + batch.getRecordCount();
            ByteBuffer buffer =
                    records.getMemorySegment()
                            .wrap(records.getPosition(), records.sizeInBytes())
                            .order(ByteOrder.LITTLE_ENDIAN);
            int firstWord = buffer.getInt(arrowOffset);
            int metadataBytes =
                    firstWord == -1 ? 8 + buffer.getInt(arrowOffset + 4) : 4 + firstWord;
            assertThat(batch.getRecordCount()).isEqualTo(3);
            assertThat(builder.getArrowMetadataSizeInBytes()).isEqualTo(metadataBytes).isPositive();
            assertThat(builder.getArrowBodySizeInBytes()).isPositive();
            assertThat(headerBytes + 3 + metadataBytes + builder.getArrowBodySizeInBytes())
                    .isEqualTo(records.sizeInBytes());
            assertThat(builder.build().sizeInBytes()).isEqualTo(records.sizeInBytes());
            assertLogRecordsEqualsWithRowKind(
                    DATA1_ROW_TYPE,
                    records,
                    Arrays.asList(
                            Tuple2.of(ChangeType.INSERT, new Object[] {1, "initial value"}),
                            Tuple2.of(ChangeType.UPDATE_BEFORE, new Object[] {1, "initial value"}),
                            Tuple2.of(ChangeType.UPDATE_AFTER, new Object[] {1, "updated value"})));
        } finally {
            builder.deallocate();
        }
    }

    @Test
    void testEmptyBatchSizesDoNotLeakFromRecycledWriter() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 0);
        LazyMemorySegmentPool pool = LazyMemorySegmentPool.createWriterBufferPool(conf);
        ArrowWalBuilder populated = (ArrowWalBuilder) createWalBuilder(tb, 1024, pool);
        populated.append(ChangeType.INSERT, row(1, "value"));
        populated.build();
        int previousMetadata = populated.getArrowMetadataSizeInBytes();
        int previousBody = populated.getArrowBodySizeInBytes();
        int previousUncompressedBody = populated.getArrowUncompressedBodySizeInBytes();
        populated.deallocate();
        ArrowWalBuilder empty = (ArrowWalBuilder) createWalBuilder(tb, 1024, pool);
        try {
            MemoryLogRecords records = empty.build();
            LogRecordBatch batch = records.batches().iterator().next();
            assertThat(batch.getRecordCount()).isZero();
            assertThat(empty.getArrowMetadataSizeInBytes()).isZero();
            assertThat(empty.getArrowBodySizeInBytes()).isZero();
            assertThat(empty.getArrowUncompressedBodySizeInBytes()).isZero();
            assertThat(records.sizeInBytes()).isEqualTo(recordBatchHeaderSize(batch.magic()));
            assertThat(populated.getArrowMetadataSizeInBytes())
                    .isEqualTo(previousMetadata)
                    .isPositive();
            assertThat(populated.getArrowBodySizeInBytes()).isEqualTo(previousBody).isPositive();
            assertThat(populated.getArrowUncompressedBodySizeInBytes())
                    .isEqualTo(previousUncompressedBody)
                    .isPositive();
        } finally {
            empty.deallocate();
        }
    }

    @Test
    void testUncompressedBodySizeForEachCompressionCodec() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 0);
        LazyMemorySegmentPool pool = LazyMemorySegmentPool.createWriterBufferPool(conf);
        int uncompressedSize = -1;
        for (ArrowCompressionInfo compression :
                Arrays.asList(
                        NO_COMPRESSION,
                        DEFAULT_COMPRESSION,
                        new ArrowCompressionInfo(ArrowCompressionType.LZ4_FRAME, -1))) {
            ArrowWalBuilder builder =
                    new ArrowWalBuilder(
                            DEFAULT_SCHEMA_ID,
                            arrowWriterProvider.getOrCreateWriter(
                                    tb.getTableId(),
                                    DEFAULT_SCHEMA_ID,
                                    1024 * 1024,
                                    DATA1_ROW_TYPE,
                                    compression),
                            pool);
            try {
                for (int i = 0; i < 256; i++) {
                    builder.append(
                            ChangeType.INSERT,
                            row(
                                    i,
                                    "repeated repeated repeated repeated repeated repeated repeated repeated"));
                }
                builder.build();
                int raw = builder.getArrowUncompressedBodySizeInBytes();
                assertThat(raw).isPositive();
                if (compression == NO_COMPRESSION) {
                    uncompressedSize = raw;
                    assertThat(builder.getArrowBodySizeInBytes()).isEqualTo(raw);
                } else {
                    assertThat(raw).isEqualTo(uncompressedSize);
                    assertThat(builder.getArrowBodySizeInBytes()).isLessThan(raw);
                }
            } finally {
                builder.deallocate();
            }
        }
    }

    private WalBuilder createWalBuilder(
            TableBucket tb, int maxSizeInBytes, MemorySegmentPool memorySegmentPool)
            throws IOException {
        return new ArrowWalBuilder(
                DEFAULT_SCHEMA_ID,
                arrowWriterProvider.getOrCreateWriter(
                        tb.getTableId(),
                        DEFAULT_SCHEMA_ID,
                        maxSizeInBytes,
                        DATA1_ROW_TYPE,
                        DEFAULT_COMPRESSION),
                memorySegmentPool);
    }
}
