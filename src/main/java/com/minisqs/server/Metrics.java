package com.minisqs.server;

import com.minisqs.core.Broker;
import com.minisqs.core.QueueStats;

import java.util.List;

/** Renders broker state in the Prometheus text format so it can be scraped and graphed. */
final class Metrics {
    private Metrics() {}

    static String render(Broker broker) {
        List<QueueStats> queues = broker.listQueues();
        StringBuilder sb = new StringBuilder();

        sb.append("# HELP minisqs_messages Messages currently in a queue, by state.\n");
        sb.append("# TYPE minisqs_messages gauge\n");
        for (QueueStats s : queues) {
            line(sb, "minisqs_messages", s.name(), "visible", s.visible());
            line(sb, "minisqs_messages", s.name(), "in_flight", s.inFlight());
            line(sb, "minisqs_messages", s.name(), "delayed", s.delayed());
        }
        gauge(sb, "minisqs_oldest_message_age_seconds", "Age of the oldest visible message.", queues, s -> s.oldestVisibleAgeMs() / 1000.0);
        counter(sb, "minisqs_sent_total", "Messages sent since start.", queues, s -> (double) s.sent());
        counter(sb, "minisqs_received_total", "Receives since start (includes redeliveries).", queues, s -> (double) s.received());
        counter(sb, "minisqs_deleted_total", "Messages deleted since start.", queues, s -> (double) s.deleted());
        counter(sb, "minisqs_moved_to_dlq_total", "Messages moved to the dead-letter queue since start.", queues, s -> (double) s.movedToDlq());

        sb.append("# TYPE minisqs_wal_bytes gauge\nminisqs_wal_bytes ").append(broker.walSizeBytes()).append('\n');
        sb.append("# TYPE minisqs_wal_appends_total counter\nminisqs_wal_appends_total ").append(broker.walAppends()).append('\n');
        sb.append("# TYPE minisqs_wal_fsyncs_total counter\nminisqs_wal_fsyncs_total ").append(broker.walFsyncs()).append('\n');
        return sb.toString();
    }

    private interface Value {
        double of(QueueStats s);
    }

    private static void gauge(StringBuilder sb, String name, String help, List<QueueStats> qs, Value v) {
        series(sb, name, help, "gauge", qs, v);
    }

    private static void counter(StringBuilder sb, String name, String help, List<QueueStats> qs, Value v) {
        series(sb, name, help, "counter", qs, v);
    }

    private static void series(StringBuilder sb, String name, String help, String type, List<QueueStats> qs, Value v) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        for (QueueStats s : qs) {
            sb.append(name).append("{queue=\"").append(s.name()).append("\"} ").append(v.of(s)).append('\n');
        }
    }

    private static void line(StringBuilder sb, String name, String queue, String state, long value) {
        sb.append(name).append("{queue=\"").append(queue).append("\",state=\"").append(state).append("\"} ")
                .append(value).append('\n');
    }
}
