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
package es.bsc.wdc.tracing.monitor;

import es.bsc.wdc.tracing.Loggers;
import es.bsc.wdc.tracing.monitor.events.MonitoredEvent;

import java.net.HttpURLConnection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Asynchronous queue to submitting events through POST to /events (events-api). The format of the body is a JSON array
 * (without external dependencies).
 */
public final class EventSink {

    // Configuration
    private static final int BATCH = 500;
    private static final long FLUSH_MS = 200;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    // State
    private final String endpoint;
    private final Thread worker;
    private final BlockingQueue<String> pendingQueue = new LinkedBlockingQueue<>(100_000);
    private boolean running = false;


    /**
     * Constructs a new EventSink for submitting events.
     *
     * @param api Endpoint where to submit the events.
     */
    public EventSink(String api) {
        this.endpoint = api;
        worker = new Thread(this::loop, "events-sink-" + api);
    }

    /**
     * Starts the eventsink and its worker thread.
     */
    public void start() {
        boolean start = false;
        synchronized (this) {
            if (!running) {
                running = true;
                start = true;
            } else {
                if (DEBUG) {
                    LOGGER.debug("EventSink for " + endpoint + " is already running.");
                }
            }
        }

        if (start) {
            if (DEBUG) {
                LOGGER.debug("Starting EventSink for " + endpoint + ".");
            }
            worker.setDaemon(true);
            worker.start();
        }
    }

    /**
     * Stops the EventSink and its worker thread.
     */
    public void stop() {
        synchronized (this) {
            running = false;
        }
        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        // último flush
        while (!pendingQueue.isEmpty()) {
            flushOnce();
        }
        if (DEBUG) {
            LOGGER.debug("Stopping EventSink for " + this.endpoint);
        }
    }

    /**
     * Adds an event to be published by the EventSink.
     * 
     * @param event Description of the event to publish
     */
    public void enqueue(MonitoredEvent event) {

        try {
            pendingQueue.offer(event.toString());
        } catch (Exception e) {
            if (DEBUG) {
                LOGGER.error("ERROR enqueuing to EventSink for " + this.endpoint, e);
            }
        }
    }

    private void loop() {
        while (running) {
            try {
                flushOnce();
                Thread.sleep(FLUSH_MS);
            } catch (InterruptedException ie) {
                // Do nothing. The loop will exit if running is false
            }
        }
    }

    private void flushOnce() {
        try {
            if (pendingQueue.isEmpty()) {
                return;
            }
            java.util.ArrayList<String> list = new java.util.ArrayList<>(BATCH);
            pendingQueue.drainTo(list, BATCH);
            if (list.isEmpty()) {
                return;
            }

            String body = "[" + String.join(",", list) + "]";

            HttpURLConnection conn = null;
            try {
                java.net.URL url = new java.net.URL(endpoint);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setDoOutput(true);
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);

                try (java.io.OutputStream os = conn.getOutputStream()) {
                    os.write(body.getBytes("UTF-8"));
                }

                int responseCode = conn.getResponseCode();

                if (responseCode >= 400) {
                    try (java.io.BufferedReader br =
                        new java.io.BufferedReader(new java.io.InputStreamReader(conn.getErrorStream()))) {
                        StringBuilder sbErr = new StringBuilder();
                        String line;
                        while ((line = br.readLine()) != null) {
                            sbErr.append(line);
                        }
                    }
                }
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
        } catch (Throwable t) {
            // LOGGER.debug("[EventSink] Error en flushOnce: " + t);
        }
    }

}
