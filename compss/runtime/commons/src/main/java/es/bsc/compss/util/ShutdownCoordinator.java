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
package es.bsc.compss.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.types.exceptions.NonInstantiableException;

/**
 * Coordinates the master shutdown between the runtime stop and the JVM shutdown hooks.
 */
public class ShutdownCoordinator {

    // Default bound to wait for the runtime stop, in milliseconds
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 120_000L;
    // Bound to wait for the runtime stop, in milliseconds, from the property compss.shutdown.timeout (in seconds)
    private static final long SHUTDOWN_TIMEOUT_MS;

    static {
        long timeoutMs = DEFAULT_SHUTDOWN_TIMEOUT_MS;
        String value = System.getProperty(COMPSsConstants.SHUTDOWN_TIMEOUT);
        if (value != null && !value.isEmpty()) {
            try {
                timeoutMs = Long.parseLong(value) * 1_000L;
            } catch (NumberFormatException e) {
                // Malformed value: keep the default timeout
            }
        }
        SHUTDOWN_TIMEOUT_MS = timeoutMs;
    }

    // State of the runtime and of its stop
    private static boolean runtimeStarted = false;
    private static boolean stopStarted = false;
    private static boolean stopFinished = false;
    // Whether a wait for the stop already timed out: later waits return at once, so the bound is not added twice
    private static boolean stopWaitTimedOut = false;


    /**
     * Private constructor to avoid instantiation.
     */
    private ShutdownCoordinator() {
        throw new NonInstantiableException("ShutdownCoordinator");
    }

    /**
     * Marks that the runtime has started: from now on, the JVM shutdown hooks wait for its stop (done by the master
     * shutdown hook if nothing else stops the runtime first).
     */
    public static synchronized void markRuntimeStarted() {
        runtimeStarted = true;
    }

    /**
     * Marks that a runtime stop has started.
     */
    public static synchronized void markStopStarted() {
        stopStarted = true;
    }

    /**
     * Marks that the runtime stop has finished and wakes up all the waiting hooks.
     */
    public static synchronized void markStopFinished() {
        stopFinished = true;
        ShutdownCoordinator.class.notifyAll();
    }

    /**
     * Reports whether a runtime stop has started.
     *
     * @return {@code true} if the runtime stop has started, {@code false} otherwise.
     */
    public static synchronized boolean isStopStarted() {
        return stopStarted;
    }

    /**
     * Reports whether the runtime stop has finished.
     *
     * @return {@code true} if the runtime stop has finished, {@code false} otherwise.
     */
    public static synchronized boolean isStopFinished() {
        return stopFinished;
    }

    /**
     * Waits for the runtime stop to finish, if the runtime started or a stop was started. Returns immediately when the
     * runtime never started or when the stop already finished. Waiting on a started runtime, not only on a started
     * stop, covers the JVM starting all the shutdown hooks at once: a hook may get here before the master hook starts
     * the stop. Once a wait timed out, later waits return {@code false} at once.
     *
     * @param timeoutMs Maximum time to wait, in milliseconds.
     * @return {@code true} if no stop is pending or the stop finished, {@code false} if the timeout expired.
     */
    public static synchronized boolean awaitStop(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while ((runtimeStarted || stopStarted) && !stopFinished) {
            long remaining = deadline - System.currentTimeMillis();
            if (stopWaitTimedOut || remaining <= 0) {
                // Timeout expired while the stop was still running: wake up the other waiters too
                stopWaitTimedOut = true;
                ShutdownCoordinator.class.notifyAll();
                return false;
            }
            try {
                ShutdownCoordinator.class.wait(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return stopFinished;
            }
        }
        return true;
    }

    /**
     * Waits for the runtime stop to finish using the shutdown timeout property.
     *
     * @return {@code true} if no stop is pending or the stop finished, {@code false} if the timeout expired.
     */
    public static boolean awaitStop() {
        return awaitStop(SHUTDOWN_TIMEOUT_MS);
    }

}
