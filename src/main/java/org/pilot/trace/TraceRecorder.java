package org.pilot.trace;

import io.opentelemetry.sdk.OpenTelemetrySdk;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Records trace relationships and context propagation for analysis
 */
public class TraceRecorder {
    private static final String TRACE_FILE = "/Users/lizhenyu/Desktop/trace.txt";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static String startingTraceId = "";

    // 无锁队列用于存储待写入的日志
    private static final ConcurrentLinkedQueue<String> writeQueue = new ConcurrentLinkedQueue<>();

    // 单线程执行器用于异步写入
    private static final ScheduledExecutorService writerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "TraceRecorder-Writer");
        t.setDaemon(true);
        return t;
    });

    // 标记是否已经关闭
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    static {
        // 初始化文件
        initializeTraceFile();

        // 初始化 OpenTelemetry
        try {
            OpenTelemetrySdk.builder().buildAndRegisterGlobal();
        } catch (IllegalStateException e) {
            System.out.println("GlobalOpenTelemetry was already initialized: " + e.getMessage());
        }

        // 启动异步写入任务，每50ms执行一次
        writerExecutor.scheduleWithFixedDelay(() -> {
            if (!shutdown.get()) {
                flushQueue();
            }
        }, 0, 50, TimeUnit.MILLISECONDS);

        // 注册关闭钩子，确保程序退出时写入所有剩余数据
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown();
        }));
    }

    /**
     * Record when a method is entered
     */
    public static void recordMethodEntry(String traceId, String spanId, String methodName) {
        if(traceId.equals(startingTraceId)){
            record(String.format("%s|ENTRY|%s|%s|%d\n",
                    traceId, spanId, methodName, System.currentTimeMillis()));
        }
    }

    /**
     * Record a relationship between parent and child spans
     */
    public static void recordRelation(String traceId, String parentSpanId,
                                      String childSpanId, String parentMethod,
                                      String childMethod, String edgeType) {
        if(traceId.equals(startingTraceId)){
            record(String.format("%s|RELATION|%s|%s|%s|%s|%s|%d\n",
                    traceId, parentSpanId, childSpanId, parentMethod, childMethod, edgeType, System.currentTimeMillis()));
        }
    }

    /**
     * Record when an async task is submitted to an executor
     */
    public static void recordAsyncSubmission(String traceId, String parentSpanId,
                                             String parentMethod, String taskClass,
                                             String submissionType) {
        if(traceId.equals(startingTraceId)){
            record(String.format("%s|ASYNC_SUBMISSION|%s|%s|%s|%s|%d\n",
                    traceId, parentSpanId, parentMethod, taskClass, submissionType, System.currentTimeMillis()));
        }
    }

    /**
     * Record when an async task starts execution
     */
    public static void recordAsyncExecution(String traceId, String parentSpanId,
                                            String childSpanId, String parentMethod,
                                            String executorThread, String taskType) {
        if(traceId.equals(startingTraceId)){
            record(String.format("%s|ASYNC_EXECUTION|%s|%s|%s|%s|%s|%d\n",
                    traceId, parentSpanId, childSpanId, parentMethod, executorThread, taskType, System.currentTimeMillis()));
        }
    }

    /**
     * Record context propagation across threads
     */
    public static void recordContextPropagation(String traceId, String fromSpanId,
                                                String toSpanId, String fromThread,
                                                String toThread, String propagationType) {
        if(traceId.equals(startingTraceId)){
            record(String.format("%s|CONTEXT_PROPAGATION|%s|%s|%s|%s|%s|%d\n",
                    traceId, fromSpanId, toSpanId, fromThread, toThread, propagationType, System.currentTimeMillis()));
        }
    }

    /**
     * Record Future callback registration
     */
    public static void recordFutureCallback(String traceId, String parentSpanId,
                                            String parentMethod, String callbackType,
                                            String edgeType) {
        if(traceId.equals(startingTraceId)){
            record(String.format("%s|FUTURE_CALLBACK|%s|%s|%s|%s|%d\n",
                    traceId, parentSpanId, parentMethod, callbackType, edgeType, System.currentTimeMillis()));
        }
    }

    /**
     * Record parent-child span relationship
     */
    public static void recordSpanRelation(String traceId, String parentSpanId,
                                          String childSpanId, String childMethod) {
        if(traceId.equals(startingTraceId)){
            record(String.format("%s|SPAN_RELATION|%s|%s|%s|%d\n",
                    traceId, parentSpanId, childSpanId, childMethod, System.currentTimeMillis()));
        }
    }

    /**
     * Write a line to the queue (无锁操作)
     */
    private static void record(String line) {
        if (!shutdown.get()) {
            writeQueue.offer(line);

            // 如果队列太大，立即触发一次写入
            if (writeQueue.size() > 10000) {
                writerExecutor.submit(() -> flushQueue());
            }
        }
    }

    /**
     * 批量写入队列中的数据到文件
     */
    private static void flushQueue() {
        List<String> batch = new ArrayList<>();
        String line;

        // 批量获取数据，最多1000条
        while ((line = writeQueue.poll()) != null && batch.size() < 1000) {
            batch.add(line);
        }

        if (!batch.isEmpty()) {
            try (FileWriter fw = new FileWriter(TRACE_FILE, true)) {
                for (String l : batch) {
                    fw.write(l);
                }
                fw.flush();
            } catch (IOException e) {
                System.err.println("Failed to write trace batch: " + e.getMessage());
                // 写入失败时，将数据重新加入队列
                batch.forEach(writeQueue::offer);
            }
        }
    }

    /**
     * Get current timestamp as formatted string
     */
    private static String getCurrentTimestamp() {
        return dateFormat.format(new Date());
    }

    /**
     * Clear the trace file (useful for starting fresh)
     */
    public static void clearTraceFile() {
        // 清空队列
        writeQueue.clear();

        try (FileWriter fw = new FileWriter(TRACE_FILE, false)) {
            fw.write("# Trace Relations Log\n");
            fw.write("# Format: traceId|eventType|...fields...|timestamp\n");
            fw.write("# Event Types:\n");
            fw.write("#   - ENTRY: Method entry point\n");
            fw.write("#   - RELATION: Parent-child span relationship\n");
            fw.write("#   - SPAN_RELATION: Direct span parent-child relationship\n");
            fw.write("#   - ASYNC_SUBMISSION: Task submitted to executor\n");
            fw.write("#   - ASYNC_EXECUTION: Task started execution\n");
            fw.write("#   - CONTEXT_PROPAGATION: Context propagated across threads\n");
            fw.write("#   - FUTURE_CALLBACK: Future callback registered\n");
            fw.write("#   - RPC_CALL: Remote procedure call\n");
            fw.write("# Timestamp is in milliseconds since epoch\n");
            fw.write("#\n");
            fw.flush();
        } catch (IOException e) {
            System.err.println("Failed to clear trace file: " + e.getMessage());
        }
    }

    /**
     * Initialize trace file with headers if it doesn't exist
     */
    private static void initializeTraceFile() {
        try {
            if (!Files.exists(Paths.get(TRACE_FILE))) {
                clearTraceFile();
            }
        } catch (Exception e) {
            System.err.println("Failed to initialize trace file: " + e.getMessage());
        }
    }

    /**
     * 强制刷新所有剩余数据并关闭
     */
    public static void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            try {
                // 最后一次刷新所有数据
                flushQueue();

                // 关闭执行器
                writerExecutor.shutdown();
                if (!writerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    writerExecutor.shutdownNow();
                }

                // 再次检查并写入剩余数据
                if (!writeQueue.isEmpty()) {
                    flushQueue();
                }
            } catch (InterruptedException e) {
                writerExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 获取当前队列大小（用于监控）
     */
    public static int getQueueSize() {
        return writeQueue.size();
    }
}