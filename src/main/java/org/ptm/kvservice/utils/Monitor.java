package org.ptm.kvservice.utils;


import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.checkerframework.checker.units.qual.A;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Monitor {

    private static final long HIGHEST_TRACKABLE_VALUE_DEFAULT = 10000000;
    private static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 3;
    private final Map<String, Histogram> histograms;
    private long startTime;
    private long stopTime;
    private long latestCountGetFromSnapShot;
    private final AtomicBoolean isRunning;
    private final int batchSize;
    private AtomicLong tTime;
    private AtomicLong sTime;

    public Monitor(Set<String> metricNames, int batchSize) {
        this.histograms = new ConcurrentHashMap<>();
        metricNames.forEach(metricName -> this.histograms.put(metricName, new ConcurrentHistogram(HIGHEST_TRACKABLE_VALUE_DEFAULT,
                NUMBER_OF_SIGNIFICANT_VALUE_DIGITS)));
        this.isRunning = new AtomicBoolean(false);
        this.batchSize = batchSize;
        this.tTime = new AtomicLong(0);
        this.sTime = new AtomicLong(0);
    }

    public void start() {
        startTime = System.currentTimeMillis();
        isRunning.set(true);
    }

    public void stop() {
        this.isRunning.set(false);
        stopTime = System.currentTimeMillis();
    }

    public void recordValue(String metricName, long value) {
        if (!isRunning.get()) {
            throw new RuntimeException("Monitor is stopped");
        }
        this.histograms.get(metricName).recordValue(value);
    }

    public AtomicLong getsTime() {
        return sTime;
    }

    public AtomicLong gettTime() {
        return tTime;
    }

    public long getCount(String metricName) {
        return this.histograms.get(metricName).getTotalCount();
    }

    public JsonObject getThroughput() {
        JsonObject snapshotAll = new JsonObject();
        this.histograms.forEach((name, histogram) -> {
            JsonObject snapshot = new JsonObject();
            long totalCount = histogram.getTotalCount() * batchSize;
            snapshot.add("TOTAL_COUNT", new JsonPrimitive(totalCount ));
            snapshot.add("TOTAL_TIME_DIVIDES_SERVER_TIME", new JsonPrimitive(this.tTime.get() * 1.0 / this.sTime.get()));
            snapshot.add("THROUGHPUT", new JsonPrimitive(totalCount * 1000.0 / (System.currentTimeMillis()
                    - startTime) ));
            snapshot.add("FROM_LAST_GET", new JsonPrimitive(totalCount - latestCountGetFromSnapShot));
            latestCountGetFromSnapShot = totalCount;
            snapshotAll.add(name, snapshot);
        });
        return snapshotAll;

    }

    public JsonObject getSnapshots() {
        JsonObject snapshotAll = new JsonObject();
        this.histograms.forEach((name, histogram) -> {
            JsonObject snapshot = new JsonObject();
            long totalCount = histogram.getTotalCount() * batchSize;
            snapshot.add("TOTAL_COUNT", new JsonPrimitive(totalCount));
            snapshot.add("THROUGHPUT", new JsonPrimitive(totalCount * 1000.0 / (System.currentTimeMillis()
                    - startTime) ));
            snapshot.add("FROM_LAST_GET", new JsonPrimitive(totalCount - latestCountGetFromSnapShot));
            latestCountGetFromSnapShot = totalCount;
            snapshot.add("MAX_VALUE", new JsonPrimitive(histogram.getMaxValue()));
            snapshot.add("MIN_VALUE", new JsonPrimitive(histogram.getMinValue()));
            snapshot.add("MEAN_VALUE", new JsonPrimitive(histogram.getMean()));
            snapshot.add("90_PERCENTILE", new JsonPrimitive(histogram.getValueAtPercentile(90.0)));
            snapshot.add("95_PERCENTILE", new JsonPrimitive(histogram.getValueAtPercentile(95.0)));
            snapshot.add("99_PERCENTILE", new JsonPrimitive(histogram.getValueAtPercentile(99.0)));
            snapshotAll.add(name, snapshot);
        });
        return snapshotAll;
    }

    public boolean isRunning() {
        return isRunning.get();
    }


}
