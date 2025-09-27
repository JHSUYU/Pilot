package org.pilot.trace;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

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
    private static final String TRACE_FILE = "/opt/trace.txt";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static String startingTraceId = "3617d8ae962ce21488ca1c968e2bd3fd";
    public static String pilotStartingTraceId = "6617d8ae962ce21488ca1c968e2bd3fd";
    public static String defaultSpanId="0000000000000000";

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

    // 标记是否已经初始化 - 使用volatile保证可见性
    private static volatile boolean initialized = false;

    // 用于双重检查锁定的对象
    private static final Object initLock = new Object();

    // ================== OpenTelemetry Helper Methods ==================
    // 这些方法用于字节码插桩，避免直接调用接口静态方法导致的兼容性问题

    /**
     * 获取当前 Span
     * 用于替代 Span.current() 的直接调用
     */
    public static Span getCurrentSpan() {
        try {
            return Span.current();
        } catch (Exception e) {
            System.err.println("Failed to get current span: " + e.getMessage());
            return null;
        }
    }

    /**
     * 从 Context 获取 Span
     */
    public static Span getSpanFromContext(Context context) {
        try {
            return Span.fromContext(context);
        } catch (Exception e) {
            System.err.println("Failed to get span from context: " + e.getMessage());
            return null;
        }
    }

    /**
     * 获取当前 Context
     */
    public static Context getCurrentContext() {
        try {
            return Context.current();
        } catch (Exception e) {
            System.err.println("Failed to get current context: " + e.getMessage());
            return null;
        }
    }

    /**
     * 获取 Span 的 SpanContext
     */
    public static SpanContext getSpanContext(Span span) {
        if (span == null) {
            return null;
        }
        try {
            return span.getSpanContext();
        } catch (Exception e) {
            System.err.println("Failed to get span context: " + e.getMessage());
            return null;
        }
    }

    /**
     * 获取 SpanId
     */
    public static String getSpanId(SpanContext spanContext) {
        if (spanContext == null) {
            return "unknown";
        }
        try {
            return spanContext.getSpanId();
        } catch (Exception e) {
            System.err.println("Failed to get span id: " + e.getMessage());
            return "unknown";
        }
    }

    /**
     * 获取 TraceId
     */
    public static String getTraceId(SpanContext spanContext) {
        if (spanContext == null) {
            return "unknown";
        }
        try {
            return spanContext.getTraceId();
        } catch (Exception e) {
            System.err.println("Failed to get trace id: " + e.getMessage());
            return "unknown";
        }
    }

    /**
     * 获取全局 Tracer
     */
    public static Tracer getTracer(String name) {
        try {
            OpenTelemetry otel = GlobalOpenTelemetry.get();
            return otel.getTracer(name);
        } catch (Exception e) {
            System.err.println("Failed to get tracer: " + e.getMessage());
            return null;
        }
    }

    /**
     * 创建并启动一个新的 Span
     */
    public static Span startSpan(String spanName) {
        try {
            Tracer tracer = getTracer("experiment-tracer");
            if (tracer == null) {
                return null;
            }
            return tracer.spanBuilder(spanName)
                    .setParent(getCurrentContext())
                    .startSpan();
        } catch (Exception e) {
            System.err.println("Failed to start span: " + e.getMessage());
            return null;
        }
    }

    /**
     * 使 Span 成为当前活动 Span
     */
    public static Scope makeCurrent(Span span) {
        if (span == null) {
            return null;
        }
        try {
            return span.makeCurrent();
        } catch (Exception e) {
            System.err.println("Failed to make span current: " + e.getMessage());
            return null;
        }
    }

    /**
     * 结束 Span
     */
    public static void endSpan(Span span) {
        if (span != null) {
            try {
                span.end();
            } catch (Exception e) {
                System.err.println("Failed to end span: " + e.getMessage());
            }
        }
    }

    /**
     * 关闭 Scope
     */
    public static void closeScope(Scope scope) {
        if (scope != null) {
            try {
                scope.close();
            } catch (Exception e) {
                System.err.println("Failed to close scope: " + e.getMessage());
            }
        }
    }

    /**
     * 安全地清理 Span 和 Scope
     */
    public static void safeCleanup(Scope scope, Span span) {
        closeScope(scope);
        endSpan(span);
    }

    // ================== Original Methods ==================

    /**
     * Initialize OpenTelemetry with singleton pattern
     * 使用双重检查锁定(Double-Checked Locking)确保线程安全且高效
     */
    public static void initializeOpenTelemetry() {
        // 第一次检查，避免不必要的同步
        if (initialized) {
            System.out.println("OpenTelemetry is already initialized, skipping...");
            return;
        }

        // 同步块，确保只有一个线程能执行初始化
        synchronized (initLock) {
            // 第二次检查，防止多个线程同时通过第一次检查
            if (initialized) {
                System.out.println("OpenTelemetry is already initialized, skipping...");
                return;
            }

            try {
                // 执行实际的初始化逻辑
                performInitialization();

                // 标记为已初始化
                initialized = true;

                System.out.println("OpenTelemetry initialized successfully");

            } catch (Exception e) {
                System.err.println("Failed to initialize OpenTelemetry: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Failed to initialize OpenTelemetry", e);
            }
        }
    }

    /**
     * 执行实际的初始化工作
     * 将原来的初始化逻辑抽取到单独的方法中
     */
    private static void performInitialization() {
        try {
            // 配置日志导出器
            CustomLoggingSpanExporter loggingExporter = CustomLoggingSpanExporter.create();

//            // 创建 SpanProcessor
//            SpanProcessor spanProcessor = BatchSpanProcessor.create(loggingExporter);
//
//            // 构建 TracerProvider
//            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
//                    .addSpanProcessor(spanProcessor)
//                    .setResource(Resource.create(Attributes.of(
//                            AttributeKey.stringKey("service.name"), "cassandra-repair-service"
//                    )))
//                    .build();

            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(
                            BatchSpanProcessor.builder(CustomLoggingSpanExporter.create())
                                    .setMaxQueueSize(2048)
                                    .setScheduleDelay(5, TimeUnit.SECONDS)  // 每5秒批量导出一次
                                    .setMaxExportBatchSize(512)  // 每批最多512个spans
                                    .build()
                    )
                    .build();

            // 注册全局 OpenTelemetry
            OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .buildAndRegisterGlobal();

            System.out.println("OpenTelemetry initialized with LoggingSpanExporter");

        } catch (IllegalStateException e) {
            // 如果已经被其他地方初始化了，记录但不抛出异常
            System.out.println("GlobalOpenTelemetry was already initialized: " + e.getMessage());
        }

        // 启动定时刷新任务
        writerExecutor.scheduleWithFixedDelay(() -> {
            if (!shutdown.get()) {
                flushQueue();
            }
        }, 0, 50, TimeUnit.MILLISECONDS);

        // 清空trace文件
        recreateTraceFile();

        // 注册关闭钩子，确保程序退出时写入所有剩余数据
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown();
        }, "TraceRecorder-ShutdownHook"));
    }

    /**
     * 检查是否已经初始化
     * @return true if initialized, false otherwise
     */
    public static boolean isInitialized() {
        return initialized;
    }

    // ... 其余原有方法保持不变 ...

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


        if(traceId.equals(startingTraceId) || traceId.equals(pilotStartingTraceId)){
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
     * Clear the trace file (useful for starting fresh)
     */
    public static void recreateTraceFile() {
        // 清空队列
        writeQueue.clear();

        try {
            // 删除现有文件（如果存在）
            Files.deleteIfExists(Paths.get(TRACE_FILE));

            // 创建新的空文件
            Files.createFile(Paths.get(TRACE_FILE));

            System.out.println("Trace file deleted and recreated: " + TRACE_FILE);

        } catch (IOException e) {
            System.err.println("Failed to clear/recreate trace file: " + e.getMessage());
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