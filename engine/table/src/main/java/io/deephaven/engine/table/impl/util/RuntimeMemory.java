//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.util.annotations.TestUseOnly;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.util.List;

/**
 * Cache memory utilization.
 *
 * <p>
 * Calling Runtime.getRuntime().freeMemory() is expensive; and we may do it a lot when we have automatically computed
 * tables, such as in a partitionBy. Instead of calling the runtime directly from the performance instrumentation
 * framework, we call this class's methods; which cache the result for a configurable number of milliseconds to avoid
 * repeated calls that are not likely any different.
 * </p>
 *
 * <p>
 * Additionally, we log our JVM heap usage on a regular basis; to enable users to quickly examine their worker logs and
 * understand memory issues.
 * </p>
 */
public class RuntimeMemory {
    /** The singleton instance. */
    private static volatile RuntimeMemory instance;

    /** The logger for Jvm heap lines */
    private final Logger log;
    /** The singleton runtime object. */
    private final Runtime runtime;
    /** The format object for heap bytes */
    private final DecimalFormat commaFormat;

    /** How long between fetches of the Runtime memory (in ms). */
    private final int cacheInterval;
    /** How long between logging Jvm Heap lines (in ms). */
    private final int logInterval;

    private static class Snapshot {
        /**
         * When should we next check for free/total memory.
         */
        long nextCheck;
        /**
         * When should we next log free/total/max memory.
         */
        long nextLog;
        /**
         * What is the last free memory value we retrieved from the Runtime.
         */
        long lastFreeMemory;
        /**
         * What is the last total memory value we retrieved from the Runtime.
         */
        long lastTotalMemory;

        /**
         * The total number of GC collections since program start.
         */
        long totalCollections;
        /**
         * The approximated total time of GC collections since program start, in milliseconds.
         */
        long totalCollectionTimeMs;

        private void readInto(Sample buf) {
            buf.freeMemory = lastFreeMemory;
            buf.totalMemory = lastTotalMemory;
            buf.totalCollections = totalCollections;
            buf.totalCollectionTimeMs = totalCollectionTimeMs;
        }
    }

    private volatile Snapshot currSnapshot;

    /** The runtime provided max memory (at startup, because it should not change). */
    private final long maxMemory;

    private List<GarbageCollectorMXBean> gcBeans;

    private RuntimeMemory(final Logger log) {
        this.log = log;
        this.runtime = Runtime.getRuntime();
        logInterval = Configuration.getInstance().getIntegerWithDefault("RuntimeMemory.logIntervalMillis", 60 * 1000);
        cacheInterval = Configuration.getInstance().getIntegerWithDefault("RuntimeMemory.cacheIntervalMillis", 1);
        maxMemory = runtime.maxMemory();

        commaFormat = new DecimalFormat();
        commaFormat.setGroupingUsed(true);

        currSnapshot = new Snapshot();
        currSnapshot.nextLog = System.currentTimeMillis() + logInterval;
        // Technically these /could/ change any time; we assume they don't.
        gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    }

    /**
     * Return a singleton RuntimeMemory object.
     *
     * @return the RuntimeMemory singleton
     */
    public static RuntimeMemory getInstance() {
        // I don't really care if we make more than one and overwrite this at startup.
        if (instance == null) {
            instance = new RuntimeMemory(LoggerFactory.getLogger(RuntimeMemory.class));
        }
        return instance;
    }

    public static class Sample {
        /**
         * What is the last free memory value we retrieved from the {@link Runtime#freeMemory()}.
         */
        public long freeMemory;
        /**
         * What is the last total memory value we retrieved from the {@link Runtime#totalMemory()}.
         */
        public long totalMemory;
        /**
         * The total number of GC collections since program start. The sum of
         * {@link GarbageCollectorMXBean#getCollectionCount()}.
         */
        public long totalCollections;
        /**
         * The approximated total time of GC collections since program start, in milliseconds. The sum of the
         * {@link GarbageCollectorMXBean#getCollectionTime()}.
         */
        public long totalCollectionTimeMs;

        /**
         * Reset all sample data to zero.
         */
        void reset() {
            freeMemory = totalMemory = totalCollections = totalCollectionTimeMs = 0L;
        }

        /**
         * Copy all the sample data in this instance to the corresponding fields in the provided instance.
         *
         * @param s destination for the copied data
         */
        void copy(final Sample s) {
            freeMemory = s.freeMemory;
            totalMemory = s.totalMemory;
            totalCollections = s.totalCollections;
            totalCollectionTimeMs = s.totalCollectionTimeMs;
        }

        /**
         * How much heap memory is used according to this sample.
         * 
         * @return the total memory minus the free memory
         */
        public long usedHeapMemory() {
            return totalMemory - freeMemory;
        }

        @Override
        public String toString() {
            final DecimalFormat format = new DecimalFormat("###,###");
            return "Sample{" +
                    "usedMemory (calculated)=" + format.format(usedHeapMemory()) +
                    ", freeMemory=" + format.format(freeMemory) +
                    ", totalMemory=" + format.format(totalMemory) +
                    ", totalCollections=" + format.format(totalCollections) +
                    ", totalCollectionTimeMs=" + format.format(totalCollectionTimeMs) +
                    '}';
        }
    }

    /**
     * Read last collected samples. Triggers a new snapshot if the last snapshot is older than {@code cacheInterval}.
     *
     * @param buf a user provided buffer object to store the samples.
     */
    public void read(final Sample buf) {
        final long now = System.currentTimeMillis();
        Snapshot snapshot = currSnapshot;
        if (now >= snapshot.nextCheck) {
            synchronized (this) {
                if (now >= currSnapshot.nextCheck) {
                    snapshot = new Snapshot();
                    snapshot.lastFreeMemory = runtime.freeMemory();
                    snapshot.lastTotalMemory = runtime.totalMemory();
                    snapshot.nextCheck = now + cacheInterval;
                    long collections = 0;
                    long collectionsMs = 0;
                    for (final GarbageCollectorMXBean gcBean : gcBeans) {
                        collections += gcBean.getCollectionCount();
                        collectionsMs += gcBean.getCollectionTime();
                    }
                    snapshot.totalCollections = collections;
                    snapshot.totalCollectionTimeMs = collectionsMs;
                    snapshot.nextLog = currSnapshot.nextLog;
                    currSnapshot = snapshot;
                }
            }
        }
        snapshot.readInto(buf);
        if (logInterval > 0 && now >= snapshot.nextLog) {
            synchronized (this) {
                if (now >= currSnapshot.nextLog) {
                    log.info().append("Jvm Heap: ").append(commaFormat.format(buf.freeMemory)).append(" Free / ")
                            .append(commaFormat.format(buf.totalMemory)).append(" Total (")
                            .append(commaFormat.format(maxMemory)).append(" Max)").endl();
                    currSnapshot.nextLog = now + logInterval;
                }
            }
        }
    }

    /**
     * Read last collected samples.
     *
     * @param buf a user provided buffer object to store the samples.
     */
    public void readOnly(final Sample buf) {
        currSnapshot.readInto(buf);
    }

    /**
     * See {@link Runtime#maxMemory()}.
     */
    public long maxMemory() {
        return maxMemory;
    }

    /**
     * How long, in millis, do we cache the samples
     */
    @TestUseOnly
    public int getCacheIntervalMillis() {
        return cacheInterval;
    }
}
