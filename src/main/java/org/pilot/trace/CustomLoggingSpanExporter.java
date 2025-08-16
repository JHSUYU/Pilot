package org.pilot.trace;


import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class CustomLoggingSpanExporter implements SpanExporter {
    private static final Logger logger = LoggerFactory.getLogger(CustomLoggingSpanExporter.class);

    public static CustomLoggingSpanExporter create() {
        return new CustomLoggingSpanExporter();
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        for (SpanData span : spans) {
            String parentSpanId = span.getParentSpanId();

            boolean hasParent = SpanId.isValid(parentSpanId) && !parentSpanId.equals(SpanId.getInvalid());

            // 自定义日志格式，包含父 span ID
//            logger.info("TRACE: {} | SPAN: {} | PARENT: {} | Name: {} | Duration: {}ms | Attributes: {}",
//                    span.getTraceId(),
//                    span.getSpanId(),
//                    hasParent ? parentSpanId : "ROOT",  // 如果没有父 span，标记为 ROOT
//                    span.getName(),
//                    (span.getEndEpochNanos() - span.getStartEpochNanos()) / 1_000_000,
//                    span.getAttributes()
//            );
        }
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        return CompletableResultCode.ofSuccess();
    }
}
