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

package org.apache.fluss.client.metrics;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.AllocationListener;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/** The metric group for scanner, including {@link LogScanner} and {@link BatchScanner}. */
@Internal
public class ScannerMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "scanner";
    private static final int WINDOW_SIZE = 1024;

    private final TablePath tablePath;

    private final Counter fetchRequestCount;
    private final Histogram bytesPerRequest;

    private final AtomicLong sourceArrowAllocatedBytes = new AtomicLong();
    private final AtomicLong sourceArrowPeakAllocatedBytes = new AtomicLong();
    private final Counter sourceArrowAllocatedBytesTotal = new ThreadSafeSimpleCounter();
    private final Counter sourceArrowReleasedBytesTotal = new ThreadSafeSimpleCounter();

    private final AllocationListener sourceArrowAllocationListener =
            new AllocationListener() {
                @Override
                public void onAllocation(long size) {
                    long allocated = sourceArrowAllocatedBytes.addAndGet(size);
                    sourceArrowPeakAllocatedBytes.accumulateAndGet(allocated, Math::max);
                    sourceArrowAllocatedBytesTotal.inc(size);
                }

                @Override
                public void onRelease(long size) {
                    sourceArrowAllocatedBytes.addAndGet(-size);
                    sourceArrowReleasedBytesTotal.inc(size);
                }
            };

    // remote log
    private final Counter remoteFetchBytes;
    private final Counter remoteFetchRequestCount;
    private final Counter remoteFetchErrorCount;

    private volatile long fetchLatencyInMs;
    private volatile long timeMsBetweenPoll;
    private volatile double pollIdleRatio;
    private volatile long lastPollMs;
    private volatile long pollStartMs;

    public ScannerMetricGroup(ClientMetricGroup parent, TablePath tablePath) {
        super(parent.getMetricRegistry(), makeScope(parent, NAME), parent);
        this.tablePath = tablePath;

        fetchRequestCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.SCANNER_FETCH_RATE, new MeterView(fetchRequestCount));

        remoteFetchBytes = new ThreadSafeSimpleCounter();
        meter(MetricNames.SCANNER_REMOTE_FETCH_BYTES_RATE, new MeterView(remoteFetchBytes));
        remoteFetchRequestCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.SCANNER_REMOTE_FETCH_RATE, new MeterView(remoteFetchRequestCount));
        remoteFetchErrorCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.SCANNER_REMOTE_FETCH_ERROR_RATE, new MeterView(remoteFetchErrorCount));

        bytesPerRequest =
                histogram(
                        MetricNames.SCANNER_BYTES_PER_REQUEST,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));

        gauge(MetricNames.SCANNER_TIME_MS_BETWEEN_POLL, () -> timeMsBetweenPoll);
        gauge(MetricNames.SCANNER_LAST_POLL_SECONDS_AGO, this::lastPollSecondsAgo);
        gauge(MetricNames.SCANNER_FETCH_LATENCY_MS, () -> fetchLatencyInMs);
        gauge(MetricNames.SCANNER_POLL_IDLE_RATIO, () -> pollIdleRatio);
        gauge(MetricNames.SCANNER_SOURCE_ARROW_ALLOCATED_BYTES, sourceArrowAllocatedBytes::get);
        gauge(
                MetricNames.SCANNER_SOURCE_ARROW_PEAK_ALLOCATED_BYTES,
                sourceArrowPeakAllocatedBytes::get);
        counter(
                MetricNames.SCANNER_SOURCE_ARROW_ALLOCATED_BYTES_TOTAL,
                sourceArrowAllocatedBytesTotal);
        counter(
                MetricNames.SCANNER_SOURCE_ARROW_RELEASED_BYTES_TOTAL,
                sourceArrowReleasedBytesTotal);
    }

    /** Registers retained local Fetch payload metrics, whose lifetime ends on drain, not poll. */
    public void registerSourceFetchMetrics(
            Counter receivedBytes, Gauge<Long> retainedBytes, Gauge<Integer> bufferedFetches) {
        counter(MetricNames.SCANNER_SOURCE_FETCH_RECEIVED_BYTES_TOTAL, receivedBytes);
        gauge(MetricNames.SCANNER_SOURCE_FETCH_RETAINED_ENCODED_BYTES, retainedBytes);
        gauge(MetricNames.SCANNER_SOURCE_BUFFERED_FETCHES, bufferedFetches);
    }

    /** Returns the shared listener for all local/remote Arrow read contexts of this scanner. */
    public AllocationListener sourceArrowAllocationListener() {
        return sourceArrowAllocationListener;
    }

    public Counter fetchRequestCount() {
        return fetchRequestCount;
    }

    public Histogram bytesPerRequest() {
        return bytesPerRequest;
    }

    public Counter remoteFetchBytes() {
        return remoteFetchBytes;
    }

    public Counter remoteFetchRequestCount() {
        return remoteFetchRequestCount;
    }

    public Counter remoteFetchErrorCount() {
        return remoteFetchErrorCount;
    }

    /**
     * Registers the gauge for the number of log records that have not been fetched. It must be
     * called at most once, otherwise the duplicated registration will be ignored with a warning.
     */
    public void registerRecordsLagGauge(Gauge<Long> recordsLagGauge) {
        gauge(MetricNames.SCANNER_RECORDS_LAG, recordsLagGauge);
    }

    public void recordPollStart(long pollStartMs) {
        this.pollStartMs = pollStartMs;
        this.timeMsBetweenPoll = lastPollMs != 0L ? pollStartMs - lastPollMs : 0L;
        this.lastPollMs = pollStartMs;
    }

    public void recordPollEnd(long pollEndMs) {
        long pollTimeMs = pollEndMs - pollStartMs;
        this.pollIdleRatio = pollTimeMs * 1.0 / (pollTimeMs + timeMsBetweenPoll);
    }

    public void updateFetchLatency(long latencyInMs) {
        fetchLatencyInMs = latencyInMs;
    }

    private long lastPollSecondsAgo() {
        return (System.currentTimeMillis() - lastPollMs) / 1000;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put("database", tablePath.getDatabaseName());
        variables.put("table", tablePath.getTableName());
    }
}
