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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.Test;

/**
 * Unit tests for {@link MasterWatchdog#runWatchdogLoop}.
 * <p>
 * All tests use a short interval (10 ms) so they complete in well under a second.
 */
public class MasterWatchdogTest {

    private static final long INTERVAL_MS = 10L;
    private static final int CONNECT_TIMEOUT_MS = 500;


    private ServerSocket openServer() throws IOException {
        return new ServerSocket(0);
    }

    /**
     * Returns a port that produces immediate ECONNREFUSED. Binds then immediately releases so no process holds it.
     * Portable across OS — does not rely on privileged ports or firewall assumptions.
     */
    private int closedPort() throws IOException {
        try (ServerSocket tmp = openServer()) {
            return tmp.getLocalPort();
        }
    }

    private Thread startLoop(int port, long intervalMs, int maxFailures, BooleanSupplier isFinished,
        Runnable onUnreachable) {
        Thread t = new Thread(() -> MasterWatchdog.runWatchdogLoop("localhost", port, intervalMs, CONNECT_TIMEOUT_MS,
            maxFailures, isFinished, onUnreachable));
        t.start();
        return t;
    }

    private static void joinDone(Thread t, long ms) throws InterruptedException {
        t.join(ms);
        assertFalse("watchdog thread must have exited", t.isAlive());
    }

    // -------------------------------------------------------------------------
    // 1. Reachable master: onUnreachable must never be called
    // -------------------------------------------------------------------------
    @Test(timeout = 3000)
    public void testReachableMasterNeverTriggersShutdown() throws Exception {
        try (ServerSocket server = openServer()) {
            int port = server.getLocalPort();
            AtomicBoolean finished = new AtomicBoolean(false);
            AtomicBoolean shutdownCalled = new AtomicBoolean(false);

            Thread t = startLoop(port, INTERVAL_MS, 3, finished::get, () -> {
                shutdownCalled.set(true);
                finished.set(true);
            });

            Thread.sleep(INTERVAL_MS * 5);
            finished.set(true);
            joinDone(t, 1000);

            assertFalse("selfShutdown must not be called when master is reachable", shutdownCalled.get());
        }
    }

    // -------------------------------------------------------------------------
    // 2. Unreachable master: onUnreachable called after exactly maxFailures
    // -------------------------------------------------------------------------
    @Test(timeout = 3000)
    public void testUnreachableMasterTriggersShutdownAfterMaxFailures() throws Exception {
        int port = closedPort();
        int maxFailures = 3;
        AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        AtomicInteger callCount = new AtomicInteger(0);

        Thread t = startLoop(port, INTERVAL_MS, maxFailures, () -> false, () -> {
            callCount.incrementAndGet();
            shutdownCalled.set(true);
        });
        joinDone(t, 2500);

        assertTrue("selfShutdown must be called when master is unreachable", shutdownCalled.get());
        assertEquals("onUnreachable must be called exactly once", 1, callCount.get());
    }

    // -------------------------------------------------------------------------
    // 2b. Boundary: maxFailures=1 triggers shutdown on the very first failure
    // -------------------------------------------------------------------------
    @Test(timeout = 3000)
    public void testSingleFailureTriggersShutdownWhenMaxFailuresIsOne() throws Exception {
        int port = closedPort();
        AtomicBoolean shutdownCalled = new AtomicBoolean(false);

        Thread t = startLoop(port, INTERVAL_MS, 1, () -> false, () -> shutdownCalled.set(true));
        joinDone(t, 2500);

        assertTrue("maxFailures=1: first failure must trigger shutdown immediately", shutdownCalled.get());
    }

    // -------------------------------------------------------------------------
    // 3. Already-finished worker: loop exits before probing
    // -------------------------------------------------------------------------
    @Test(timeout = 3000)
    public void testAlreadyFinishedWorkerExitsImmediately() throws Exception {
        AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        int port = closedPort();

        Thread t = startLoop(port, INTERVAL_MS, 3, () -> true, () -> shutdownCalled.set(true));
        joinDone(t, 500);

        assertFalse("selfShutdown must not be called when already finished", shutdownCalled.get());
    }

    // -------------------------------------------------------------------------
    // 3b. isFinished becomes true between sleep and probe: skips the probe
    // -------------------------------------------------------------------------
    @Test(timeout = 3000)
    public void testIsFinishedAfterSleepSkipsProbe() throws Exception {
        int port = closedPort();
        AtomicBoolean finished = new AtomicBoolean(false);
        AtomicBoolean shutdownCalled = new AtomicBoolean(false);

        // isFinished returns false on the first call (while-guard) and true on the second (post-sleep guard),
        // so the loop exits without attempting a connection.
        int[] calls = { 0 };
        Thread t = new Thread(() -> MasterWatchdog.runWatchdogLoop("localhost", port, INTERVAL_MS, CONNECT_TIMEOUT_MS,
            3, () -> calls[0]++ > 0, () -> shutdownCalled.set(true)));
        t.start();
        joinDone(t, 1000);

        assertFalse("selfShutdown must not be called when isFinished returns true after sleep", shutdownCalled.get());
    }

    // -------------------------------------------------------------------------
    // 4. Master comes back: failure counter resets; shutdown is not triggered
    // -------------------------------------------------------------------------
    @Test(timeout = 5000)
    public void testTransientFailureResetsCounter() throws Exception {
        final long interval = 50L;
        final int maxFailures = 4;

        int port = closedPort();
        AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        AtomicBoolean finished = new AtomicBoolean(false);

        Thread watchdog = startLoop(port, interval, maxFailures, finished::get, () -> shutdownCalled.set(true));

        // Let maxFailures-1 probes fail, then open the server so the next probe succeeds and resets the counter.
        Thread.sleep(interval * (maxFailures - 1) + interval / 2);

        try (ServerSocket server = new ServerSocket()) {
            server.setReuseAddress(true);
            server.bind(new java.net.InetSocketAddress(port));

            Thread.sleep(interval * 3);
            finished.set(true);
        }

        watchdog.join(1000);
        assertFalse("counter reset: (maxFailures-1) failures followed by success must not trigger shutdown",
            shutdownCalled.get());
    }

    // -------------------------------------------------------------------------
    // 4b. Boundary: maxFailures-1 failures alone must NOT trigger shutdown
    // -------------------------------------------------------------------------
    @Test(timeout = 3000)
    public void testFewerThanMaxFailuresDoesNotTriggerShutdown() throws Exception {
        final long interval = 50L;
        final int maxFailures = 3;
        int port = closedPort();

        AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        AtomicBoolean finished = new AtomicBoolean(false);

        Thread t = startLoop(port, interval, maxFailures, finished::get, () -> shutdownCalled.set(true));

        Thread.sleep(interval * (maxFailures - 2) + interval / 2);
        finished.set(true);
        joinDone(t, 1000);

        assertFalse("(maxFailures-1) consecutive failures must NOT trigger shutdown", shutdownCalled.get());
    }

    // -------------------------------------------------------------------------
    // 5. Thread interruption: loop exits cleanly without calling onUnreachable
    // -------------------------------------------------------------------------
    @Test(timeout = 3000)
    public void testInterruptedThreadExitsCleanly() throws Exception {
        AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        int port = closedPort();

        Thread t = startLoop(port, 5000L, 3, () -> false, () -> shutdownCalled.set(true));
        Thread.sleep(50);
        t.interrupt();
        joinDone(t, 500);

        assertFalse("selfShutdown must not be called on interrupt", shutdownCalled.get());
    }
}
