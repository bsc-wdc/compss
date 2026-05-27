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
package es.bsc.compss.components.monitor.impl;

import es.bsc.compss.log.Loggers;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Asynchronous sink for graph events.
 */
public final class GraphSink {

    private static final int BATCH = 500;
    private static final long FLUSH_MS = 200;

    private static final Logger LOGGER = LogManager.getLogger(Loggers.ALL_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private final String endpoint;
    private final Thread worker;
    private final BlockingQueue<String> pendingQueue = new LinkedBlockingQueue<>(100_000);

    private boolean running = false;


    /**
     * Constructs a new GraphSink.
     *
     * @param api endpoint where graph events are submitted
     */
    public GraphSink(String api) {
        this.endpoint = api;
        this.worker = new Thread(this::loop, "graph-sink-" + api);
    }

    /**
     * Starts the sink worker.
     */
    public void start() {
        boolean start = false;
        synchronized (this) {
            if (!this.running) {
                this.running = true;
                start = true;
            }
        }
        if (start) {
            if (DEBUG) {
                LOGGER.debug("Starting GraphSink for " + this.endpoint + ".");
            }
            this.worker.setDaemon(true);
            this.worker.start();
        }
    }

    /**
     * Stops the sink worker and flushes pending events.
     */
    public void stop() {
        synchronized (this) {
            this.running = false;
        }
        this.worker.interrupt();
        try {
            this.worker.join(1000);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        while (!this.pendingQueue.isEmpty()) {
            flushOnce();
        }
        if (DEBUG) {
            LOGGER.debug("Stopping GraphSink for " + this.endpoint + ".");
        }
    }

    /**
     * Enqueues a graph event JSON object.
     *
     * @param json event JSON
     */
    public void enqueue(String json) {
        if (json != null) {
            this.pendingQueue.offer(json);
        }
    }

    private void loop() {
        while (this.running) {
            try {
                flushOnce();
                Thread.sleep(FLUSH_MS);
            } catch (InterruptedException ie) {
                // The loop will exit when running is false.
            }
        }
    }

    private void flushOnce() {
        try {
            if (this.pendingQueue.isEmpty()) {
                return;
            }
            ArrayList<String> list = new ArrayList<>(BATCH);
            this.pendingQueue.drainTo(list, BATCH);
            if (list.isEmpty()) {
                return;
            }

            String body = "[" + String.join(",", list) + "]";
            HttpURLConnection conn = null;
            try {
                java.net.URL url = new java.net.URL(this.endpoint);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setDoOutput(true);
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);

                try (java.io.OutputStream os = conn.getOutputStream()) {
                    os.write(body.getBytes("UTF-8"));
                }
                conn.getResponseCode();
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
        } catch (Throwable t) {
            if (DEBUG) {
                LOGGER.debug("GraphSink flush failed", t);
            }
        }
    }
}
