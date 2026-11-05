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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the lifecycle of materialized log handover metrics. */
class FlinkRecordsWithSplitIdsTest {

    @Test
    void testConsumeAndRecycleIsIdempotent() {
        InterceptingOperatorMetricGroup group = new InterceptingOperatorMetricGroup();
        FlinkSourceReaderMetrics metrics =
                new FlinkSourceReaderMetrics(InternalSourceReaderMetricGroup.wrap(group));
        AtomicInteger closes = new AtomicInteger();
        FlinkRecordsWithSplitIds records = records(metrics, closes, false);
        assertThat(queuedRecords(group)).isEqualTo(2L);
        assertThat(records.nextSplit()).isEqualTo("split");
        assertThat(records.nextRecordFromSplit().record().logOffset()).isEqualTo(0L);
        assertThat(queuedRecords(group)).isEqualTo(1L);
        records.recycle();
        records.recycle();
        assertThat(closes.get()).isEqualTo(1);
        assertThat(queuedRecords(group)).isZero();
        assertThat(((Gauge<?>) group.get("sourceQueuedBatches")).getValue()).isEqualTo(0L);
    }

    @Test
    void testStoppingOffsetAndFailingRecycleReleaseMetrics() {
        InterceptingOperatorMetricGroup group = new InterceptingOperatorMetricGroup();
        FlinkSourceReaderMetrics metrics =
                new FlinkSourceReaderMetrics(InternalSourceReaderMetricGroup.wrap(group));
        AtomicInteger closes = new AtomicInteger();
        FlinkRecordsWithSplitIds records = records(metrics, closes, true);
        records.setTableBucketStoppingOffset(new TableBucket(1L, 0), 1L);
        records.nextSplit();
        assertThat(records.nextRecordFromSplit()).isNotNull();
        assertThat(records.nextRecordFromSplit()).isNull();
        assertThat(queuedRecords(group)).isZero();
        assertThatThrownBy(records::recycle).isInstanceOf(IllegalStateException.class);
        records.recycle();
        assertThat(closes.get()).isEqualTo(1);
        assertThat(((Gauge<?>) group.get("sourceQueuedBatches")).getValue()).isEqualTo(0L);
    }

    @Test
    void testFailingFirstSplitCloseStillClosesRemainingSplits() {
        InterceptingOperatorMetricGroup group = new InterceptingOperatorMetricGroup();
        FlinkSourceReaderMetrics metrics =
                new FlinkSourceReaderMetrics(InternalSourceReaderMetricGroup.wrap(group));
        AtomicInteger firstCloses = new AtomicInteger();
        AtomicInteger secondCloses = new AtomicInteger();
        Map<String, CloseableIterator<RecordAndPos>> splits = new LinkedHashMap<>();
        splits.put("first", iterator(firstCloses, true));
        splits.put("second", iterator(secondCloses, false));
        FlinkRecordsWithSplitIds records =
                new FlinkRecordsWithSplitIds(
                        splits,
                        splits.keySet().iterator(),
                        Arrays.asList(new TableBucket(1L, 0), new TableBucket(1L, 1)).iterator(),
                        Collections.emptySet(),
                        metrics.registerQueuedBatch(4));

        assertThatThrownBy(records::recycle)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("close failed");
        assertThat(firstCloses.get()).isEqualTo(1);
        assertThat(secondCloses.get()).isEqualTo(1);
        assertThat(queuedRecords(group)).isZero();
        assertThat(((Gauge<?>) group.get("sourceQueuedBatches")).getValue()).isEqualTo(0L);

        records.recycle();
        assertThat(firstCloses.get()).isEqualTo(1);
        assertThat(secondCloses.get()).isEqualTo(1);
    }

    private static long queuedRecords(InterceptingOperatorMetricGroup group) {
        return (long) ((Gauge<?>) group.get("sourceQueuedRecords")).getValue();
    }

    private static FlinkRecordsWithSplitIds records(
            FlinkSourceReaderMetrics metrics, AtomicInteger closes, boolean failClose) {
        return new FlinkRecordsWithSplitIds(
                Collections.singletonMap("split", iterator(closes, failClose)),
                Collections.singleton("split").iterator(),
                Collections.singleton(new TableBucket(1L, 0)).iterator(),
                Collections.emptySet(),
                metrics.registerQueuedBatch(2));
    }

    private static CloseableIterator<RecordAndPos> iterator(
            AtomicInteger closes, boolean failClose) {
        Iterator<RecordAndPos> input =
                Arrays.asList(
                                new RecordAndPos(new ScanRecord(0L, 0L, ChangeType.INSERT, row(1))),
                                new RecordAndPos(new ScanRecord(1L, 0L, ChangeType.INSERT, row(2))))
                        .iterator();
        return new CloseableIterator<RecordAndPos>() {
            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public RecordAndPos next() {
                return input.next();
            }

            @Override
            public void close() {
                closes.incrementAndGet();
                if (failClose) {
                    throw new IllegalStateException("close failed");
                }
            }
        };
    }
}
