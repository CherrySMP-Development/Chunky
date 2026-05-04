package org.popcraft.chunky;

import org.popcraft.chunky.api.event.task.GenerationCompleteEvent;
import org.popcraft.chunky.api.event.task.GenerationProgressEvent;
import org.popcraft.chunky.event.task.GenerationTaskFinishEvent;
import org.popcraft.chunky.event.task.GenerationTaskUpdateEvent;
import org.popcraft.chunky.iterator.ChunkIterator;
import org.popcraft.chunky.iterator.ChunkIteratorFactory;
import org.popcraft.chunky.platform.Sender;
import org.popcraft.chunky.shape.Shape;
import org.popcraft.chunky.shape.ShapeFactory;
import org.popcraft.chunky.util.ChunkCoordinate;
import org.popcraft.chunky.util.Input;
import org.popcraft.chunky.util.Pair;
import org.popcraft.chunky.util.RegionCache;
import org.popcraft.chunky.util.TranslationKey;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class GenerationTask implements Runnable {
    private static final double SAMPLE_INTERVAL = 1000d * Math.max(Input.tryInteger(System.getProperty("chunky.sampleInterval")).orElse(30), 30);
    private static final double SAMPLE_SUB_INTERVAL = SAMPLE_INTERVAL / 30;
    private final Chunky chunky;
    private final Selection selection;
    private final Shape shape;
    private final AtomicLong startTime = new AtomicLong();
    private final AtomicLong updateTime = new AtomicLong();
    private final AtomicLong finishedChunks = new AtomicLong();
    private final Deque<Pair<Long, AtomicLong>> updateSamples = new ConcurrentLinkedDeque<>();
    private final Progress progress;
    private final RegionCache.WorldState worldState;
    private ChunkIterator chunkIterator;
    private boolean stopped, cancelled;
    private long prevTime;

    public GenerationTask(final Chunky chunky, final Selection selection, final long count, final long time, final boolean cancelled) {
        this(chunky, selection);
        this.chunkIterator = ChunkIteratorFactory.getChunkIterator(selection, count);
        this.finishedChunks.set(count);
        this.cancelled = cancelled;
        this.prevTime = time;
    }

    public GenerationTask(final Chunky chunky, final Selection selection) {
        this.chunky = chunky;
        this.selection = selection;
        this.chunkIterator = ChunkIteratorFactory.getChunkIterator(selection);
        this.shape = ShapeFactory.getShape(selection);
        this.progress = new Progress(selection.world().getName());
        this.worldState = chunky.getRegionCache().getWorld(selection.world().getName());
    }

    private synchronized void update(final int chunkX, final int chunkZ, final boolean loaded) {
        if (stopped) {
            return;
        }
        progress.chunkCount = finishedChunks.addAndGet(1);
        progress.percentComplete = 100f * progress.chunkCount / chunkIterator.total();
        final long currentTime = System.currentTimeMillis();
        final Pair<Long, AtomicLong> bin = updateSamples.peekLast();
        if (loaded) {
            worldState.setGenerated(chunkX, chunkZ);
            if (bin != null && currentTime - bin.left() < SAMPLE_SUB_INTERVAL) {
                bin.right().addAndGet(1);
            } else if (updateSamples.add(Pair.of(currentTime, new AtomicLong(1)))) {
                while (!updateSamples.isEmpty() && currentTime - updateSamples.peek().left() > SAMPLE_INTERVAL) {
                    updateSamples.poll();
                }
            }
        }
        final Pair<Long, AtomicLong> oldest = updateSamples.peek();
        final long oldestTime = oldest == null ? currentTime : oldest.left();
        final long chunksLeft = chunkIterator.total() - finishedChunks.get();
        final double timeDiff = (currentTime - oldestTime) / 1e3;
        if (chunksLeft > 0 && timeDiff < 1e-1) {
            return;
        }
        long sampleCount = 0;
        for (Pair<Long, AtomicLong> b : updateSamples) {
            sampleCount += b.right().get();
        }
        progress.rate = timeDiff > 0 ? sampleCount / timeDiff : 0;
        final long time;
        if (chunksLeft == 0) {
            time = (prevTime + (currentTime - startTime.get())) / 1000;
            progress.complete = true;
        } else {
            time = (long) (chunksLeft / progress.rate);
        }
        progress.hours = time / 3600;
        progress.minutes = (time - progress.hours * 3600) / 60;
        progress.seconds = time - progress.hours * 3600 - progress.minutes * 60;
        progress.chunkX = chunkX;
        progress.chunkZ = chunkZ;
        chunky.getEventBus().call(new GenerationProgressEvent(progress.world, progress.chunkCount, progress.complete, progress.percentComplete, progress.hours, progress.minutes, progress.seconds, progress.rate, progress.chunkX, progress.chunkZ));
        if (progress.complete) {
            progress.sendUpdate(chunky.getServer().getConsole());
            chunky.getEventBus().call(new GenerationTaskUpdateEvent(this));
            return;
        }
        final boolean silentMode = chunky.getConfig().isSilent();
        final boolean updateIntervalElapsed = ((currentTime - updateTime.get()) / 1e3) > chunky.getConfig().getUpdateInterval();
        if (updateIntervalElapsed) {
            if (!silentMode) {
                progress.sendUpdate(chunky.getServer().getConsole());
            }
            chunky.getEventBus().call(new GenerationTaskUpdateEvent(this));
            updateTime.set(currentTime);
        }
    }

    /**
     * Get the current CPU process load.
     */
    private double getCpuLoad() {
        try {
            final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            if (osBean instanceof com.sun.management.OperatingSystemMXBean extendedOsBean) {
                return extendedOsBean.getProcessCpuLoad();
            }
        } catch (Exception ignored) {
            // Default to 0 if we can't get CPU load
        }
        return 0.0;
    }

    @Override
    public void run() {
        final String poolThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(String.format("Chunky-%s Thread", selection.world().getName()));
        if (!chunkIterator.process()) {
            stop(true);
        }
        // Max CPS enforced by the rate limiter
        final double MAX_CPS = 800.0;
        final java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger(0);
        final boolean forceLoadExistingChunks = chunky.getConfig().isForceLoadExistingChunks();
        startTime.set(System.currentTimeMillis());

        // Background thread for CPU monitoring
        final Thread cpuMonitorThread = new Thread(() -> {
            boolean warnedCpu = false;
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(5000); // Check CPU every 5s
                    if (!warnedCpu) {
                        final double cpuLoad = getCpuLoad();
                        if (cpuLoad > 0.95) {
                            chunky.getServer().getConsole().sendMessage("§c[Chunky] Warning: CPU load is >95%! Generation speed is locked at 800 CPS and will not throttle. Performance may degrade.§r");
                            warnedCpu = true;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        cpuMonitorThread.setName("Chunky-CPU-Monitor");
        cpuMonitorThread.setDaemon(true);
        cpuMonitorThread.start();

        long lastChunkTime = System.nanoTime();

        while (!stopped && chunkIterator.hasNext()) {
            try {
                // Enforce maximum absolute CPS rate limit (applies to ALL chunks including skipped)
                final long minIntervalNs = (long) (1_000_000_000.0 / MAX_CPS);
                long elapsed;
                while ((elapsed = System.nanoTime() - lastChunkTime) < minIntervalNs && !stopped) {
                    if (minIntervalNs - elapsed > 2_000_000) {
                        //noinspection BusyWait
                        Thread.sleep(1);
                    } else {
                        Thread.yield();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stop(cancelled);
                break;
            }
            lastChunkTime = System.nanoTime();

            final ChunkCoordinate chunk = chunkIterator.next();
            final int chunkCenterX = (chunk.x() << 4) + 8;
            final int chunkCenterZ = (chunk.z() << 4) + 8;
            if (!shape.isBounding(chunkCenterX, chunkCenterZ)) {
                update(chunk.x(), chunk.z(), false);
                continue;
            }
            if (!forceLoadExistingChunks && worldState.isGenerated(chunk.x(), chunk.z())) {
                update(chunk.x(), chunk.z(), false);
                continue;
            }
            try {

                // Wait until we are under the max concurrent limit
                // Increased to 10000 to ensure all server worker threads are fully saturated
                while (inFlight.get() >= 10000 && !stopped) {
                    //noinspection BusyWait
                    Thread.sleep(2);
                }
                inFlight.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stop(cancelled);
                break;
            }
            final CompletableFuture<Boolean> isChunkGenerated = forceLoadExistingChunks ?
                    CompletableFuture.completedFuture(false) :
                    selection.world().isChunkGenerated(chunk.x(), chunk.z());
            isChunkGenerated
                    .thenCompose(generated -> {
                        if (Boolean.TRUE.equals(generated)) {
                            return CompletableFuture.completedFuture(null);
                        } else {
                            return selection.world().getChunkAtAsync(chunk.x(), chunk.z());
                        }
                    }).whenComplete((ignored, ignored2) -> {
                        inFlight.decrementAndGet();
                        update(chunk.x(), chunk.z(), true);
                    });
        }
        try {
            cpuMonitorThread.interrupt();
            cpuMonitorThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (stopped) {
            chunky.getServer().getConsole().sendMessagePrefixed(TranslationKey.TASK_STOPPED, selection.world().getName());
        } else {
            cancelled = true;
        }
        chunky.getTaskLoader().saveTask(this);
        chunky.getGenerationTasks().remove(selection.world().getName());
        Thread.currentThread().setName(poolThreadName);
        chunky.getEventBus().call(new GenerationTaskFinishEvent(this));
        chunky.getEventBus().call(new GenerationCompleteEvent(selection.world().getName()));
    }

    public void stop(final boolean cancelled) {
        this.stopped = true;
        this.cancelled = cancelled;
    }

    public Chunky getChunky() {
        return chunky;
    }

    public Selection getSelection() {
        return selection;
    }

    public long getCount() {
        return finishedChunks.get();
    }

    public ChunkIterator getChunkIterator() {
        return chunkIterator;
    }

    public Shape getShape() {
        return shape;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public long getTotalTime() {
        return prevTime + (startTime.get() > 0 ? System.currentTimeMillis() - startTime.get() : 0);
    }

    public Progress getProgress() {
        return progress;
    }

    @SuppressWarnings("unused")
    public static final class Progress {
        private final String world;
        private long chunkCount;
        private boolean complete;
        private float percentComplete;
        private long hours, minutes, seconds;
        private double rate;
        private int chunkX, chunkZ;

        private Progress(final String world) {
            this.world = world;
        }

        public String getWorld() {
            return world;
        }

        public long getChunkCount() {
            return chunkCount;
        }

        public boolean isComplete() {
            return complete;
        }

        public float getPercentComplete() {
            return percentComplete;
        }

        public long getHours() {
            return hours;
        }

        public long getMinutes() {
            return minutes;
        }

        public long getSeconds() {
            return seconds;
        }

        public double getRate() {
            return rate;
        }

        public int getChunkX() {
            return chunkX;
        }

        public int getChunkZ() {
            return chunkZ;
        }

        public void sendUpdate(final Sender sender) {
            if (complete) {
                sender.sendMessagePrefixed(TranslationKey.TASK_DONE, world, chunkCount, String.format("%.2f", percentComplete), String.format("%01d", hours), String.format("%02d", minutes), String.format("%02d", seconds));
            } else {
                sender.sendMessagePrefixed(TranslationKey.TASK_UPDATE, world, chunkCount, String.format("%.2f", percentComplete), String.format("%01d", hours), String.format("%02d", minutes), String.format("%02d", seconds), String.format("%.1f", rate), chunkX, chunkZ);
            }
        }
    }
}
