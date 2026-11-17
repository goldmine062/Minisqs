package com.minisqs;

import com.minisqs.server.Main;
import com.minisqs.tools.Args;
import com.minisqs.tools.CrashTest;
import com.minisqs.tools.LoadTest;
import com.minisqs.tools.Producer;
import com.minisqs.tools.SelfTest;
import com.minisqs.tools.Worker;

import java.util.Arrays;

/** Entry point: java -jar minisqs.jar <command> [--options] */
public final class Cli {
    private Cli() {}

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            usage();
            return;
        }
        String[] rest = Arrays.copyOfRange(argv, 1, argv.length);
        switch (argv[0]) {
            case "server" -> Main.main(rest);
            case "produce" -> Producer.run(new Args(rest));
            case "worker" -> Worker.run(new Args(rest));
            case "loadtest" -> LoadTest.run(new Args(rest));
            case "crashtest-send" -> CrashTest.send(new Args(rest));
            case "crashtest-verify" -> CrashTest.verify(new Args(rest));
            case "selftest" -> SelfTest.run();
            default -> usage();
        }
    }

    private static void usage() {
        System.out.println("""
                Usage: java -jar minisqs.jar <command> [options]

                  server            start the broker      --port 8080 --data-dir data --fsync true
                  produce           send demo messages    --queue orders --count 20 --interval-ms 500
                  worker            consume messages      --queue orders --name w1 --work-ms 1000 --fail-rate 0 --crash-after -1
                  loadtest          measure throughput    --producers 8 --consumers 8 --seconds 20 --batch 10
                  crashtest-send    send and record acks  --count 20000
                  crashtest-verify  check nothing was lost after a crash
                  selftest          run the test suite
                """);
    }
}
