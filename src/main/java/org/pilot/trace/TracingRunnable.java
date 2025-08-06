package org.pilot.trace;

import io.opentelemetry.api.trace.Span;

public class TracingRunnable implements Runnable {
    private final Runnable delegate;
    private final String traceId;
    private final String parentSpanId;
    private final String parentMethod;

    public TracingRunnable(Runnable delegate, String traceId,
                           String parentSpanId, String parentMethod) {
        this.delegate = delegate;
        this.traceId = traceId;
        this.parentSpanId = parentSpanId;
        this.parentMethod = parentMethod;
    }

    @Override
    public void run() {
        Span current = Span.current();
        if (current.getSpanContext().isValid()) {
            TraceRecorder.recordRelation(
                    traceId,
                    parentSpanId,
                    current.getSpanContext().getSpanId(),
                    parentMethod,
                    Thread.currentThread().getStackTrace()[2].toString(),
                    "THREAD_POOL"
            );
        }

        delegate.run();
    }
}
