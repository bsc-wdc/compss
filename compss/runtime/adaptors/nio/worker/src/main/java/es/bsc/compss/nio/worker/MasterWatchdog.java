/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.nio.worker;

import es.bsc.compss.log.Loggers;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Daemon thread that periodically probes TCP reachability of the master. If the master becomes permanently unreachable
 * (e.g. killed by SIGKILL before sending CommandShutdown), the worker initiates a graceful self-shutdown so it does not
 * hang indefinitely on TM.join().
 */
public class MasterWatchdog {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER);

    static final long DEFAULT_INTERVAL_MS =
        Long.parseLong(System.getProperty("compss.worker.watchdog.interval", "60000"));
    static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
    static final int DEFAULT_MAX_FAILURES =
        Integer.parseInt(System.getProperty("compss.worker.watchdog.maxFailures", "6"));


    private MasterWatchdog() {
    }

    /**
     * Creates and starts a daemon watchdog thread for the given worker.
     *
     * @param worker The NIOWorker to monitor; its {@code selfShutdown()} is called if the master is unreachable.
     * @param masterHost Hostname or IP of the master.
     * @param masterPort Port of the master.
     */
    public static void start(NIOWorker worker, String masterHost, int masterPort) {
        Thread t = new Thread() {

            @Override
            public void run() {
                runWatchdogLoop(masterHost, masterPort, DEFAULT_INTERVAL_MS, DEFAULT_CONNECT_TIMEOUT_MS,
                    DEFAULT_MAX_FAILURES, worker::isFinished, worker::selfShutdown);
            }
        };
        t.setName("MasterWatchdog");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Watchdog loop body. Periodically probes TCP reachability of the master. Calls {@code onUnreachable} exactly once
     * when {@code maxFailures} consecutive failures are reached, then returns. Package-private for testing.
     *
     * @param masterHost Master hostname.
     * @param masterPort Master port.
     * @param intervalMs Sleep interval between probes in milliseconds.
     * @param connectTimeoutMs TCP connect timeout in milliseconds.
     * @param maxFailures Number of consecutive failures before calling {@code onUnreachable}.
     * @param isFinished Returns {@code true} when the worker has already shut down.
     * @param onUnreachable Called once when the failure threshold is reached.
     */
    static void runWatchdogLoop(String masterHost, int masterPort, long intervalMs, int connectTimeoutMs,
        int maxFailures, BooleanSupplier isFinished, Runnable onUnreachable) {
        int failures = 0;
        while (!isFinished.getAsBoolean()) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(masterHost, masterPort), connectTimeoutMs);
                failures = 0;
                LOGGER.debug("[Watchdog] Master is reachable.");
            } catch (IOException e) {
                failures++;
                LOGGER.warn("[Watchdog] Master unreachable ({}/{}): {}", failures, maxFailures, e.getMessage());
                if (failures >= maxFailures) {
                    onUnreachable.run(); // called exactly once; returns immediately after
                    return;
                }
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
