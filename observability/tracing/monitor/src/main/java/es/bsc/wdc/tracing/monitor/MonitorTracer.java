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
    private static final int TASKS_FUNC_CODE = 8_001_131;
    private static final int TASK_REGISTRY_CODE = 88_000_000;


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
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    private static final class ThreadCtx {

        ThreadType threadType = ThreadType.UNKNOWN;
        long threadId = Thread.currentThread().getId();
    }


    private final String masterName;
    private final String nodeName;

    private final String runId;

    private final ThreadLocal<ThreadCtx> ctx = new ThreadLocal<>();
    private final EventSink monitoredSink;
    private final OtelMetrics metrics;
    private final Map<Integer, Map<Integer, String>> eventLabels = new HashMap<>();


    /**
     * Constructs and sets up new tracer leveraging OpenTelemetry.
     *
     * @param nodeName name of the node in the telemetry system
     * @param masterName name of the master node
     * @param runId id of the execution
     * @param serviceName name of the service that we are monitoring
     * @param metricsEP endpoint where to publish metrics
     * @param eventsEP endpoint where to publish events
     */
    MonitorTracer(String nodeName, String masterName, String runId, String serviceName, String metricsEP,
        String eventsEP) {
        this.nodeName = nodeName;
        this.masterName = masterName;
        this.runId = runId;
        this.metrics = new OtelMetrics(metricsEP, serviceName, nodeName, masterName);
        this.monitoredSink = new EventSink(eventsEP);
        this.monitoredSink.start();
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
    public final void activeComponent(int id, String description) {
        // Identify thread emitting the event
        if (id != 0L) {
            ThreadCtx c = new ThreadCtx();
            c.threadId = Thread.currentThread().getId();
            if (id == ThreadType.AP.id) {
                c.threadType = ThreadType.AP;
            } else if (id == ThreadType.TD.id) {
                c.threadType = ThreadType.TD;
            } else if (id == ThreadType.EXEC.id) {
                c.threadType = ThreadType.EXEC;
            } else {
                c.threadType = ThreadType.UNKNOWN;
            }
            ctx.set(c);
        }
    }

    @Override
    public final void inactiveComponent() {
        ctx.remove();
    }

    @Override
    public final void startSynch(long value) {
        // Do nothing
    }

    @Override
    public final void endSynch() {
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
                if (typeCode == TASKS_FUNC_CODE) {
                    MonitoredEvent registryEvent = new MonitoredEvent(runId, masterName, nodeName, "REGISTRY", 0L,
                        TASK_REGISTRY_CODE, e.getId(), e.getSignature());
                    this.monitoredSink.enqueue(registryEvent.toString());
                }
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

            ThreadCtx c = ctx.get();
            if (c == null) {
                return;
            }

            MonitoredEvent event;
            String threadType = c.threadType.name();
            String label = getEventLabel(eventType, value);
            event =
                new MonitoredEvent(runId, masterName, nodeName, threadType, c.threadId, eventType, (int) value, label);
            this.monitoredSink.enqueue(event.toString());
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
        this.monitoredSink.stop();
    }

    @Override
    public void prepareSubProcessEnvironment(Map<String, String> env, Boolean defineExtra) {
        // Do nothing
    }

    @Override
    public String getWorkerTracingConfiguration() {
        // Cannot be configured in a remote worker
        return null;
    }
}
