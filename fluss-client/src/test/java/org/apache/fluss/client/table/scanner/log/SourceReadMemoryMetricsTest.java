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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies exported reader allocation accounting across local and remote read contexts. */
class SourceReadMemoryMetricsTest {

    @Test
    void testUnshadedArrowBatchApiUsesTheSameAllocationListener() throws Exception {
        CapturingRegistry registry = new CapturingRegistry();
        ScannerMetricGroup metrics =
                new ScannerMetricGroup(
                        new ClientMetricGroup(registry, "arrow-api"), DATA1_TABLE_PATH);
        ChunkedAllocationManager.ChunkedFactory factory =
                new ChunkedAllocationManager.ChunkedFactory();
        MemoryLogRecords records =
                createRecordsWithoutBaseLogOffset(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0,
                        0,
                        CURRENT_LOG_MAGIC_VALUE,
                        DATA1,
                        LogFormat.ARROW,
                        true);
        try (LogRecordReadContext context =
                LogRecordReadContext.createReadContext(
                        DATA1_TABLE_INFO,
                        false,
                        LogRecordReadContext.SchemaResolution.TARGET,
                        null,
                        TEST_SCHEMA_GETTER,
                        factory,
                        metrics.sourceArrowAllocationListener())) {
            try (ArrowBatchData batch =
                    records.batches().iterator().next().loadArrowBatch(context)) {
                assertThat(batch.getRecordCount()).isEqualTo(DATA1.size());
                assertThat(registry.gauge("sourceArrowAllocatedBytes")).isPositive();
                assertThat(registry.counter("sourceArrowAllocatedBytesTotal")).isPositive();
            }
            assertThat(registry.gauge("sourceArrowAllocatedBytes")).isZero();
            assertThat(registry.gauge("sourceArrowPeakAllocatedBytes")).isPositive();
        } finally {
            factory.close();
            metrics.close();
        }
    }

    @Test
    void testArrowCurrentPeakAndCumulativeAllocationAcrossContexts() {
        CapturingRegistry registry = new CapturingRegistry();
        ScannerMetricGroup metrics =
                new ScannerMetricGroup(new ClientMetricGroup(registry, "reader"), DATA1_TABLE_PATH);
        ChunkedAllocationManager.ChunkedFactory factory =
                new ChunkedAllocationManager.ChunkedFactory();
        LogFetcher.TableReadContext context =
                new LogFetcher.TableReadContext(
                        DATA1_TABLE_INFO,
                        LogRecordReadContext.SchemaResolution.TARGET,
                        null,
                        null,
                        TEST_SCHEMA_GETTER,
                        factory,
                        metrics.sourceArrowAllocationListener());
        try {
            assertThat(registry.gauge("sourceArrowAllocatedBytes")).isZero();
            try (ArrowBuf first = context.readContext.getBufferAllocator().buffer(128)) {
                try (ArrowBuf second = context.remoteReadContext.getBufferAllocator().buffer(256)) {
                    assertThat(first.capacity()).isEqualTo(128);
                    assertThat(second.capacity()).isEqualTo(256);
                    assertThat(registry.gauge("sourceArrowAllocatedBytes")).isEqualTo(384);
                    assertThat(registry.gauge("sourceArrowPeakAllocatedBytes")).isEqualTo(384);
                }
                assertThat(registry.gauge("sourceArrowAllocatedBytes")).isEqualTo(128);
                try (ArrowBuf third = context.readContext.getBufferAllocator().buffer(64)) {
                    assertThat(third.capacity()).isEqualTo(64);
                    assertThat(registry.gauge("sourceArrowAllocatedBytes")).isEqualTo(192);
                    assertThat(registry.gauge("sourceArrowPeakAllocatedBytes")).isEqualTo(384);
                }
            }
            assertThat(registry.gauge("sourceArrowAllocatedBytes")).isZero();
            assertThat(registry.gauge("sourceArrowPeakAllocatedBytes")).isEqualTo(384);
            assertThat(registry.counter("sourceArrowAllocatedBytesTotal")).isEqualTo(448);
            assertThat(registry.counter("sourceArrowReleasedBytesTotal")).isEqualTo(448);
        } finally {
            context.close();
            factory.close();
            metrics.close();
        }
    }

    private static final class CapturingRegistry implements MetricRegistry {
        private final Map<String, Metric> metrics = new HashMap<>();

        long gauge(String name) {
            return ((Number) ((Gauge<?>) metrics.get(name)).getValue()).longValue();
        }

        long counter(String name) {
            return ((Counter) metrics.get(name)).getCount();
        }

        @Override
        public int getNumberReporters() {
            return 1;
        }

        @Override
        public void register(Metric metric, String name, AbstractMetricGroup group) {
            metrics.put(name, metric);
        }

        @Override
        public void unregister(Metric metric, String name, AbstractMetricGroup group) {
            metrics.remove(name);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }
    }
}
