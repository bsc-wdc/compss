/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.data.operation;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.listener.EventListener;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class DataOperation {

    private static int opCount = 0;
    private final int operationId;
    // Identifiers of the groups to which the operation belongs
    private final List<EventListener> listeners;
    private OperationEndState endState = null;
    private Exception endException = null;

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    private String name;


    /**
     * Data Operation constructor.
     * 
     * @param ld Logical data to operate
     * @param listener Data operation listener
     */
    public DataOperation(LogicalData ld, EventListener listener) {
        this.name = ld.getName();
        this.listeners = new LinkedList<>();
        this.listeners.add(listener);
        operationId = opCount;
        opCount++;
    }

    /**
     * Data Operation constructor.
     * 
     * @param ld Logical data to operate
     * @param eventListeners List of ata operation listeners
     */
    public DataOperation(LogicalData ld, List<EventListener> eventListeners) {
        this.name = ld.getName();
        this.listeners = eventListeners;
        operationId = opCount;
        opCount++;
    }

    public int getId() {
        return operationId;
    }

    public String getName() {
        return name;
    }

    public List<EventListener> getEventListeners() {
        return listeners;
    }

    /**
     * Add listener to a data operation.
     * 
     * @param eventListener Listener to add
     */
    public void addEventListener(EventListener eventListener) {
        synchronized (listeners) {
            if (endState == null) {
                this.listeners.add(eventListener);
            } else {
                switch (endState) {
                    case OP_OK:
                        eventListener.notifyEnd(this);
                        break;
                    case OP_IN_PROGRESS:
                        break;
                    case OP_WAITING_SOURCES:
                        break;
                    default: // OP_FAILED or OP_PREPARATION_FAILED
                        eventListener.notifyFailure(this, endException);
                        break;
                }
            }
        }
    }

    /**
     * Add a list of listener to a data operation.
     * 
     * @param eventListeners Listeners to add
     */
    public void addEventListeners(List<EventListener> eventListeners) {
        synchronized (listeners) {
            if (endState == null) {
                this.listeners.addAll(eventListeners);
            } else {
                switch (endState) {
                    case OP_OK:
                        for (EventListener eventListener : eventListeners) {
                            eventListener.notifyEnd(this);
                        }
                        break;
                    case OP_IN_PROGRESS:
                        break;
                    case OP_WAITING_SOURCES:
                        break;
                    default: // OP_FAILED or OP_PREPARATION_FAILED
                        for (EventListener eventListener : eventListeners) {
                            eventListener.notifyFailure(this, endException);
                        }
                        break;
                }
            }
        }
    }

    /**
     * Removes a listener from a data operation.
     *
     * @param eventListener Listener to remove
     */
    public void removeEventListener(EventListener eventListener) {
        synchronized (listeners) {
            this.listeners.remove(eventListener);
        }
    }

    public void end(OperationEndState state) {
        notifyEnd(state, null);
    }

    public void end(OperationEndState state, Exception e) {
        notifyEnd(state, e);
    }

    private void notifyEnd(OperationEndState state, Exception e) {
        synchronized (listeners) {
            endState = state;
            endException = e;
            // Check end state of the operation
            switch (state) {
                case OP_OK:
                    for (EventListener listener : listeners) {
                        listener.notifyEnd(this);
                    }
                    break;
                case OP_IN_PROGRESS:
                    break;
                case OP_WAITING_SOURCES:
                    break;
                default: // OP_FAILED or OP_PREPARATION_FAILED
                    for (EventListener listener : listeners) {
                        listener.notifyFailure(this, e);
                    }
                    break;
            }
        }
    }

    public abstract void perform();
}
