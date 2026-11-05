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

package org.apache.fluss.flink.source.metrics;

import org.apache.fluss.flink.source.reader.FlinkSourceReader;
import org.apache.fluss.metrics.Gauge;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.flink.metrics.FlinkConnectorMetricNames.IN_DATA_BYTES;
import static org.apache.fluss.flink.metrics.FlinkConnectorMetricNames.IN_DATA_BYTES_RATE;
import static org.apache.fluss.flink.metrics.FlinkConnectorMetricNames.IN_DATA_COUNT;
import static org.apache.fluss.flink.metrics.FlinkConnectorMetricNames.IN_DATA_RATE;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** A collection class for handling metrics in {@link FlinkSourceReader} of Fluss. */
public class FlinkSourceReaderMetrics {

    public static final long UNINITIALIZED = -1;

    // Source reader metric group
    private final SourceReaderMetricGroup sourceReaderMetricGroup;
    private final Set<Gauge<Long>> recordsLagMetrics = ConcurrentHashMap.newKeySet();
    private final Counter inDataCount;
    private final Counter inDataBytes;
    private final Counter numRecordsIn;
    private final Counter numBytesIn;

    // For currentFetchEventTimeLag metric
    private volatile long currentFetchEventTimeLag = UNINITIALIZED;

    private boolean pendingRecordsGaugeRegistered;

    private final Set<QueuedBatch> queuedBatches = ConcurrentHashMap.newKeySet();
    private boolean closed;

    public FlinkSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.sourceReaderMetricGroup = sourceReaderMetricGroup;
        sourceReaderMetricGroup.gauge("sourceQueuedRecords", this::getQueuedRecords);
        sourceReaderMetricGroup.gauge("sourceQueuedBatches", () -> (long) queuedBatches.size());
        inDataCount = sourceReaderMetricGroup.counter(IN_DATA_COUNT);
        sourceReaderMetricGroup.meter(IN_DATA_RATE, new MeterView(inDataCount));
        inDataBytes = sourceReaderMetricGroup.counter(IN_DATA_BYTES);
        sourceReaderMetricGroup.meter(IN_DATA_BYTES_RATE, new MeterView(inDataBytes));
        numRecordsIn = sourceReaderMetricGroup.getIOMetricGroup().getNumRecordsInCounter();
        numBytesIn = sourceReaderMetricGroup.getIOMetricGroup().getNumBytesInCounter();
    }

    /** Records a Fluss scan record consumed by this source reader. */
    public void recordConsumed(int bytes) {
        inDataCount.inc();
        numRecordsIn.inc();
        if (bytes >= 0) {
            inDataBytes.inc(bytes);
            numBytesIn.inc(bytes);
        }
    }

    public void reportRecordEventTime(long lag) {
        if (currentFetchEventTimeLag == UNINITIALIZED) {
            // Lazily register the currentFetchEventTimeLag
            // Set the lag before registering the metric to avoid metric reporter getting
            // the uninitialized value
            currentFetchEventTimeLag = lag;
            sourceReaderMetricGroup.gauge(
                    MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, () -> currentFetchEventTimeLag);
            return;
        }
        currentFetchEventTimeLag = lag;
    }

    /**
     * Adds a scanner's records-lag metric to the metrics tracked by the standard Flink
     * pendingRecords gauge. The Flink gauge itself is registered only once.
     */
    public synchronized void maybeAddRecordsLagMetric(Gauge<Long> recordsLagMetric) {
        recordsLagMetrics.add(recordsLagMetric);
        if (!pendingRecordsGaugeRegistered) {
            sourceReaderMetricGroup.setPendingRecordsGauge(this::getPendingRecords);
            pendingRecordsGaugeRegistered = true;
        }
    }

    /** Removes a scanner's records-lag metric from the tracked metrics. */
    public void removeRecordsLagMetric(Gauge<Long> recordsLagMetric) {
        recordsLagMetrics.remove(recordsLagMetric);
    }

    /**
     * Tracks a materialized log handover package until it is recycled. Lazy snapshot iterators and
     * control-only packages are excluded. The record count describes records awaiting handover, not
     * the number of references retained by their underlying lists.
     */
    public synchronized QueuedBatch registerQueuedBatch(long records) {
        checkArgument(records >= 0, "Queued record count must not be negative.");
        QueuedBatch batch = new QueuedBatch(records);
        if (!closed && records > 0) {
            queuedBatches.add(batch);
        }
        return batch;
    }

    /** Stops queue accounting and makes late fetch/recycle callbacks harmless. */
    public synchronized void close() {
        closed = true;
        queuedBatches.forEach(QueuedBatch::close);
        queuedBatches.clear();
    }

    private long getQueuedRecords() {
        long records = 0;
        for (QueuedBatch batch : queuedBatches) {
            records += batch.remaining.get();
        }
        return records;
    }

    /** Accounting token for a materialized log handover package. */
    public final class QueuedBatch implements AutoCloseable {
        private final AtomicLong remaining;

        private QueuedBatch(long records) {
            this.remaining = new AtomicLong(records);
        }

        /** Records consumption or dropping of one pending log record. */
        public void recordDequeued() {
            remaining.updateAndGet(value -> value > 0 ? value - 1 : 0);
        }

        /** Removes the package and any unconsumed records; repeated recycling is harmless. */
        @Override
        public void close() {
            remaining.set(0);
            queuedBatches.remove(this);
        }
    }

    public SourceReaderMetricGroup getSourceReaderMetricGroup() {
        return sourceReaderMetricGroup;
    }

    private long getPendingRecords() {
        long pendingRecords = 0L;
        for (Gauge<Long> recordsLagMetric : recordsLagMetrics) {
            pendingRecords += recordsLagMetric.getValue();
        }
        return pendingRecords;
    }
}
