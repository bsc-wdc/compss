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

import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import es.bsc.wdc.tracing.Loggers;
import es.bsc.wdc.tracing.TracingBackend;
import es.bsc.wdc.tracing.monitor.events.MonitoredEvent;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MonitorTracer implements TracingBackend {

    // Constants to remove
    private static final int THREAD_IDENTIFICATION_CODE = 8_001_003;


    private enum ThreadType {

        AP(2), TD(3), EXEC(8), UNKNOWN(null);


        private final Integer id;


        ThreadType(Integer id) {
            this.id = id;
        }

        public Integer getId() {
            return id;
        }
    }


    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();


    private static final class ThreadCtx {

        ThreadType threadType = ThreadType.UNKNOWN;
        long threadId = Thread.currentThread().getId();
    }


    private static final ThreadLocal<ThreadCtx> CTX = ThreadLocal.withInitial(ThreadCtx::new);
    private final String masterName;
    private final String nodeName;
    private final EventSink sink;
    private final OtelMetrics metrics;
    private final Map<Integer, Map<Integer, String>> eventLabels = new HashMap<>();


    /**
     * Constructs and sets up new tracer leveraging OpenTelemetry.
     *
     * @param masterName name of the master of the execution
     * @param hostname name of the host
     */
    public MonitorTracer(String masterName, String hostname) {
        this.masterName = masterName;
        this.nodeName = hostname;
        String serviceName = System.getProperty(Constants.ENV_OTEL_SERVICE, Constants.DEFAULT_SERVICE_NAME);
        String otlpEP = System.getProperty(Constants.ENV_OTEL_ENDPOINT, Constants.DEFAULT_OTEL_ENDPOINT);
        this.metrics = new OtelMetrics(otlpEP, serviceName, hostname, masterName);

        String eventsEP = System.getProperty(Constants.ENV_EVENTS_API, Constants.DEFAULT_EVENTS_API);
        this.sink = new EventSink(eventsEP);
        this.sink.start();
    }

    @Override
    public final void enablePThreads() {
        // Do nothing
    }

    @Override
    public final void disablePThreads() {
        // Do nothing
    }

    @Override
    public void defineEventType(EventType type) {
        int typeCode = type.getCode();
        Map<Integer, String> typeLabels;
        synchronized (this.eventLabels) {
            typeLabels = this.eventLabels.get(typeCode);
            if (typeLabels == null) {
                typeLabels = new HashMap<>();
                this.eventLabels.put(typeCode, typeLabels);
                if (type.isEndable()) {
                    typeLabels.put(0, "End/Idle");
                }
            }
        }
        synchronized (typeLabels) {
            for (Event e : type.getEvents()) {
                typeLabels.put(e.getId(), e.getSignature());
            }
        }
    }

    private String getEventLabel(int typeCode, long value) {
        while (true) {
            try {
                Map<Integer, String> typeEvents = this.eventLabels.get(typeCode);
                if (typeEvents != null) {
                    String label = typeEvents.get((int) value);
                    if (label == null) {
                        return Long.toString(value);
                    }
                    return label;
                } else {
                    return Long.toString(value);
                }
            } catch (ConcurrentModificationException cme) {
                // Will recompute it because another thread modified the map while querying it.
            }
        }
    }

    @Override
    public final void emitEvent(int eventType, long value) {
        try {
            // Identify thread emitting the event
            if (eventType == THREAD_IDENTIFICATION_CODE && value != 0L) {
                ThreadCtx c = CTX.get();
                c.threadId = Thread.currentThread().getId();
                if (value == ThreadType.AP.id) {
                    c.threadType = ThreadType.AP;
                } else if (value == ThreadType.TD.id) {
                    c.threadType = ThreadType.TD;
                } else if (value == ThreadType.EXEC.id) {
                    c.threadType = ThreadType.EXEC;
                } else {
                    c.threadType = ThreadType.UNKNOWN;
                }
                return;
            }

            ThreadCtx c = CTX.get();
            if (!(c.threadType == ThreadType.AP || c.threadType == ThreadType.TD || c.threadType == ThreadType.EXEC)) {
                // Ignore events from threads other than AP, TD and Executors
                return;
            }

            MonitoredEvent event;
            String threadType = c.threadType.name();
            String label = getEventLabel(eventType, value);
            event = new MonitoredEvent(masterName, nodeName, threadType, c.threadId, eventType, (int) value, label);
            this.sink.enqueue(event);
        } catch (Throwable t) {
            LOGGER.debug("MonitorTracer emitEvent failed", t);
        }
    }

    @Override
    public final void emitEventAndCounters(int eventType, long value) {
        this.emitEvent(eventType, value);
    }

    @Override
    public void emitCommunicationEvent(boolean send, int tag, long size, int partnerID, int ownID) {
        // Do nothing
    }

    @Override
    public void fini() {
        this.sink.stop();
    }

    @Override
    public void prepareSubProcessEnvironment(Map<String, String> env, Boolean defineExtra) {
        // Do nothing
    }
}
