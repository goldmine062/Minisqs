package com.minisqs.server;

import com.minisqs.core.Broker;

import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Starts the broker: java -jar minisqs.jar server [--port 8080] [--data-dir data] [--fsync true] */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        com.minisqs.tools.Args a = new com.minisqs.tools.Args(args);
        int port = a.integer("port", 8080);
        Path dataDir = Path.of(a.str("data-dir", "data"));
        boolean fsync = a.bool("fsync", true);
        long compactThreshold = a.integer("compact-mb", 64) * 1024L * 1024L;

        Broker broker = Broker.open(dataDir, fsync);
        Broker.RecoveryInfo r = broker.recoveryInfo();
        System.out.printf("Recovered %d queues and %d messages from %d log records in %d ms%s%n",
                r.queues(), r.messages(), r.recordsReplayed(), r.millis(),
                r.truncatedBytes() > 0 ? " (cut " + r.truncatedBytes() + " bytes of a torn write at the end of the log)" : "");

        HttpApi api = new HttpApi(broker, port);
        api.start();

        ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable, "log-compaction");
            t.setDaemon(true);
            return t;
        });
        maintenance.scheduleWithFixedDelay(() -> {
            try {
                Broker.CompactionResult c = broker.compactIfNeeded(compactThreshold);
                if (c != null) {
                    System.out.printf("Compacted log: %,d -> %,d bytes%n", c.bytesBefore(), c.bytesAfter());
                }
            } catch (Exception e) {
                System.err.println("Compaction failed: " + e);
            }
        }, 10, 10, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            api.stop();
            try {
                broker.close();
            } catch (Exception ignored) {
            }
            System.out.println("Broker stopped cleanly");
        }));

        System.out.printf("MiniSQS listening on http://localhost:%d  (data: %s, fsync: %s)%n",
                port, dataDir.toAbsolutePath(), fsync);
        System.out.printf("Dashboard: http://localhost:%d/   Metrics: http://localhost:%d/metrics%n", port, port);
    }
}
