package org.ptm.kvservice.utils;


import org.ptm.kvservice.db.FastDBIml;
import com.google.common.util.concurrent.AtomicDouble;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Stats {

    private static Stats INSTANCE = null;

    public static final double GB = 1024 * 1024 * 1024.0;
    public static final String SERVICE_NAME = "kvservice";

    private final PrometheusMeterRegistry prometheusRegistry;
    private final AtomicLong totalKey;
    private final AtomicDouble totalMemory;
    private final AtomicDouble freeMemory;
    private final AtomicDouble maxMemory;
    private final AtomicDouble sysMemory;
    private final AtomicDouble diskUsage;
    private final AtomicInteger availableProcessors;

    private final Map<String, Timer> requestsGauge;
//
//    private final Map<String, Map.Entry<AtomicLong, AtomicLong>> requestCounter;
    private final File dirFile;
//    private final AtomicLong lastTime;
    private final FastDBIml fastDBIml;

    public static Stats getINSTANCE() {
        if (INSTANCE == null) {
            throw new RuntimeException("INSTANCE need to init");
        }
        return INSTANCE;
    }

    public PrometheusMeterRegistry getPrometheusRegistry() {
        return prometheusRegistry;
    }

    public void record(String type, long totalOps, long latency) {
        long avg = latency / totalOps;
        for (int i = 0; i < totalOps; i++) {
            record(type, avg);
        }
    }
    public void record(String type, long latency) {
        this.requestsGauge.get(type).record(latency, TimeUnit.MILLISECONDS);
    }

    public static void init(String dirPath, Set<String> requestTypes, FastDBIml fastDBIml) {
        if (INSTANCE == null) {
            synchronized (Stats.class) {
                INSTANCE = new Stats(dirPath, requestTypes, fastDBIml);
            }
        }
    }

    private Stats(String dirPath, Set<String> requestTypes, FastDBIml fastDBIml) {
        this.prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        this.fastDBIml = fastDBIml;
        this.totalKey = this.prometheusRegistry.gauge("total_key", new AtomicLong(fastDBIml.totalKey()));
        this.totalMemory = this.prometheusRegistry.gauge("total_memory", new AtomicDouble());
        this.freeMemory = this.prometheusRegistry.gauge("free_memory", new AtomicDouble());
        this.maxMemory = this.prometheusRegistry.gauge("max_memory", new AtomicDouble());
        this.sysMemory = this.prometheusRegistry.gauge("sys_memory", new AtomicDouble());
        this.diskUsage = this.prometheusRegistry.gauge("disk_usage", new AtomicDouble());
        this.availableProcessors = this.prometheusRegistry.gauge("available_processors", new AtomicInteger());
        this.requestsGauge = new ConcurrentHashMap<>();
        requestTypes.forEach(type -> {
            this.requestsGauge.put(type, this.prometheusRegistry.timer(type.replace("/api/", "")));
        });
        this.dirFile = Paths.get(dirPath).toFile();
    }

    public String scrape(Set<String> included) {
        this.totalKey.set(fastDBIml.totalKey());
        this.totalMemory.set(Runtime.getRuntime().totalMemory() / GB);
        this.freeMemory.set(Runtime.getRuntime().freeMemory() / GB);
        this.maxMemory.set(Runtime.getRuntime().maxMemory() / GB);
        this.sysMemory.set(((com.sun.management.OperatingSystemMXBean)
                ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize() / GB);
        this.diskUsage.set(this.dirFile.getTotalSpace() / GB);
        this.availableProcessors.set(Runtime.getRuntime().availableProcessors());
        return this.prometheusRegistry.scrape("text/plain; version=0.0.4; charset=utf-8", included);
    }

}
