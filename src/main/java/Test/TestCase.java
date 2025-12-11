package Test;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import org.pilot.PilotUtil;
import org.pilot.concurrency.ThreadManager;
import org.pilot.trace.CustomLoggingSpanExporter;
import org.pilot.trace.TraceRecorder;
import org.pilot.zookeeper.ZooKeeperClient;

import java.util.concurrent.TimeUnit;

import static org.pilot.Constants.pilotTraceId;
import static org.pilot.PilotUtil.getPilotContextInternal;

public class TestCase {

    public static void main(String[] args){
        testStart(() -> {
            System.out.println("Pilot entry point is running.");
            try {
                // Simulate some work in the pilot
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Pilot entry point has completed.");
        });
    }

    public static Context testStart(Runnable entryPoint) {
        try {
            // 配置日志导出器
            CustomLoggingSpanExporter loggingExporter = CustomLoggingSpanExporter.create();

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

        Context tmp = Context.current();
        Context pilotContext = getPilotContextInternal(tmp, 1);

        OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
        Tracer tracer = openTelemetry.getTracer("experiment-tracer");
        String parentSpanId = IdGenerator.random().generateSpanId();

        SpanContext remoteSpanContext = SpanContext.createFromRemoteParent(
                pilotTraceId,
                parentSpanId,
                TraceFlags.getSampled(),
                TraceState.getDefault()
        );
        pilotContext = pilotContext.with(Span.wrap(remoteSpanContext));

        // ========== 打印外层 context 信息 ==========
        System.out.println("=== OUTER CONTEXT (before wrap) ===");
        System.out.println("pilotTraceId: " + pilotTraceId);
        System.out.println("parentSpanId (manually generated): " + parentSpanId);
        Span outerSpan = Span.fromContext(pilotContext);
        SpanContext outerSpanContext = outerSpan.getSpanContext();
        System.out.println("Outer Span from pilotContext - TraceId: " + outerSpanContext.getTraceId());
        System.out.println("Outer Span from pilotContext - SpanId: " + outerSpanContext.getSpanId());
        System.out.println("Outer Span from pilotContext - isValid: " + outerSpanContext.isValid());

        if (entryPoint instanceof Thread) {
            try (Scope scope = pilotContext.makeCurrent()) {
                PilotUtil.startThread((Thread) entryPoint);
            }
            return pilotContext;
        }

        System.out.println("Created pilot context for pilot ID: " + 1);

        // ========== 用带调试的 Runnable 替换 ==========
        Runnable debugRunnable = () -> {
            // ========== 检查 context 是否传播成功 ==========
            System.out.println("=== INSIDE WRAPPED RUNNABLE ===");

            Span currentSpanInside = Span.current();
            SpanContext currentCtxInside = currentSpanInside.getSpanContext();
            System.out.println("Span.current() inside - TraceId: " + currentCtxInside.getTraceId());
            System.out.println("Span.current() inside - SpanId: " + currentCtxInside.getSpanId());
            System.out.println("Span.current() inside - isValid: " + currentCtxInside.isValid());
            System.out.println("Expected parentSpanId: " + parentSpanId);
            System.out.println("Actual SpanId inside: " + currentCtxInside.getSpanId());
            System.out.println("Context propagated correctly? " + parentSpanId.equals(currentCtxInside.getSpanId()));

            // ========== 模拟插桩代码 ==========
            Span span = null;
            Scope scope = null;

            // 获取父 Span 信息（应该是外层的 remoteSpanContext）
            Span parentSpan = Span.current();
            SpanContext parentContext = parentSpan.getSpanContext();
            String capturedParentSpanId = parentContext.getSpanId();
            String capturedParentTraceId = parentContext.getTraceId();

            System.out.println("=== BEFORE creating child span ===");
            System.out.println("Captured Parent TraceId: " + capturedParentTraceId);
            System.out.println("Captured Parent SpanId: " + capturedParentSpanId);

            // 创建子 Span
            Tracer innerTracer = GlobalOpenTelemetry.get().getTracer("experiment-tracer");
            String methodName = "TestClass.testMethod";
            SpanBuilder spanBuilder = innerTracer.spanBuilder(methodName);
            spanBuilder = spanBuilder.setParent(Context.current());
            span = spanBuilder.startSpan();
            scope = span.makeCurrent();

            // 获取子 Span 信息
            SpanContext childSpanContext = span.getSpanContext();
            String childSpanId = childSpanContext.getSpanId();
            String childTraceId = childSpanContext.getTraceId();

            System.out.println("=== AFTER creating child span ===");
            System.out.println("Child TraceId: " + childTraceId);
            System.out.println("Child SpanId: " + childSpanId);

            System.out.println("=== FINAL COMPARISON ===");
            System.out.println("Parent SpanId: " + capturedParentSpanId);
            System.out.println("Child SpanId:  " + childSpanId);
            System.out.println("Are they DIFFERENT (expected)? " + !capturedParentSpanId.equals(childSpanId));
            System.out.println("Same TraceId (expected)? " + capturedParentTraceId.equals(childTraceId));

            // 记录关系
            //TraceRecorder.recordSpanRelation(childTraceId, capturedParentSpanId, childSpanId, methodName);

            // ========== 原有业务逻辑 ==========
            System.out.println("Pilot entry point is running.");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Pilot entry point has completed.");

            // ========== 清理 ==========
            if (scope != null) scope.close();
            if (span != null) span.end();
        };

        Runnable wrappedRunnable = pilotContext.wrap(debugRunnable);
        System.out.println("Wrapped runnable with pilot context for pilot ID: " + 1);

        // 执行 wrapped runnable（或者放到 phantom thread 里）
        wrappedRunnable.run();  // 或者 phantomThread 里执行

        // phantomThread.start();
        System.out.println("Phantom thread started for pilot ID: " + 1);
        return pilotContext;
    }
}