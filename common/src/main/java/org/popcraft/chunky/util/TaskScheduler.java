package org.popcraft.chunky.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskScheduler {
    private static final int DEFAULT_MAX_THREADS = Math.min(Math.max(1, Runtime.getRuntime().availableProcessors()), 16);
    private static final String PAPER_GLOBAL_CONFIG = "paper-global.yml";
    private static final int MAX_THREADS = Math.max(1, Input.tryInteger(System.getProperty("chunky.maxThreads"))
            .or(() -> Input.tryInteger(System.getProperty("Paper.WorkerThreadCount")))
            .or(TaskScheduler::readPaperWorkerThreads)
            .orElse(DEFAULT_MAX_THREADS));
    private final ExecutorService executor;
    private final Set<Future<?>> futures = ConcurrentHashMap.newKeySet();

    public TaskScheduler() {
        final int maxThreads = Math.max(1, MAX_THREADS);
        final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                maxThreads,
                maxThreads,
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>()
        );
        threadPoolExecutor.setThreadFactory(runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        });
        threadPoolExecutor.prestartAllCoreThreads();
        this.executor = threadPoolExecutor;
    }

    public void runTask(final Runnable runnable) {
        futures.add(executor.submit(runnable));
        futures.removeIf(Future::isDone);
    }

    public void cancelTasks() {
        for (Future<?> future : futures) {
            future.cancel(true);
        }
        futures.clear();
    }

    private static java.util.Optional<Integer> readPaperWorkerThreads() {
        final Path path = Path.of(PAPER_GLOBAL_CONFIG);
        if (!Files.isRegularFile(path)) {
            return java.util.Optional.empty();
        }
        try {
            boolean inChunkSystem = false;
            for (String line : Files.readAllLines(path)) {
                final String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                if (trimmed.equals("chunk-system:")) {
                    inChunkSystem = true;
                    continue;
                }
                if (!line.startsWith(" ") && !line.startsWith("\t")) {
                    inChunkSystem = false;
                }
                if (!inChunkSystem) {
                    continue;
                }
                if (trimmed.startsWith("worker-threads:")) {
                    return Input.tryInteger(trimmed.substring("worker-threads:".length()).trim());
                }
            }
        } catch (IOException ignored) {
        }
        return java.util.Optional.empty();
    }
}
