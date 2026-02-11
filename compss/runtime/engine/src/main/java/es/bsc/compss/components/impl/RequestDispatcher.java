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
package es.bsc.compss.components.impl;

import es.bsc.compss.types.request.Request;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;


public abstract class RequestDispatcher<T extends Request> implements Runnable {

    private final String threadName;
    private final Logger logger;

    // Tasks to be processed
    private final RequestQueue<T> requestQueue;

    // Processor thread
    private final Thread processor;
    private boolean keepGoing;


    /**
     * Constructs a new RequestDispatcher.
     *
     * @param threadName name of the thread dispatching requests
     * @param logger logger that will be used
     */
    public RequestDispatcher(String threadName, Logger logger) {
        this.threadName = threadName;
        this.logger = logger;
        this.requestQueue = new RequestQueue<>();

        keepGoing = true;
        processor = new Thread(this);
        processor.setName(threadName);
        if (Tracer.isActivated()) {
            Tracer.enablePThreads(1);
        }
    }

    /**
     * Starts the thread dispatching requests.
     */
    protected final void start() {
        this.processor.start();
    }

    @Override
    public final void run() {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(getThreadEvent());
            Tracer.disablePThreads(1);
        }
        while (keepGoing) {
            T request = null;
            try {
                request = this.requestQueue.take();
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(request.getEvent());
                }
                handleRequest(request);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (ShutdownException se) {
                logger.debug("Shutting down " + threadName);
                se.getSemaphore().release();
                break;
            } catch (Exception e) {
                logger.error("Error in " + threadName + " request:" + e.getMessage());
                ErrorManager.error("Exception in " + threadName + " request " + request.getEvent().toString(), e);
            } finally {
                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(request.getEvent());
                }
            }

        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(getThreadEvent());
        }
        logger.info(threadName + " shutdown");
    }

    /**
     * Adds a new request to the queue for being processed.
     *
     * @param req request
     * @param errMsg message to print if there was an error
     * @return {@literal true} if the request was properly added to the queue; {@literal false}, otherwise
     */
    protected final boolean offerRequest(T req, String errMsg) {
        if (!this.requestQueue.offer(req)) {
            String errQueueMsg = "ERROR: %s queue offer error on %s";
            String msg = String.format(errQueueMsg, threadName, errMsg);
            ErrorManager.error(msg);
            return false;
        }
        return true;
    }

    /**
     * Adds a new request to the queue for being processed with high priority.
     *
     * @param req request
     * @param errMsg message to print if there was an error
     * @return {@literal true} if the request was properly added to the queue; {@literal false}, otherwise
     */
    protected final boolean offerRequestWithPriority(T req, String errMsg) {
        if (!this.requestQueue.offerWithPriority(req)) {
            String errQueueMsg = "ERROR: %s queue offer error on %s";
            String msg = String.format(errQueueMsg, threadName, errMsg);
            ErrorManager.error(msg);
            return false;
        }
        return true;
    }

    /**
     * Obtains the event related to the thread Id of the dispatcher.
     *
     * @return event related to threadId
     */
    public abstract TraceEvent getThreadEvent();

    /**
     * Method for handling one of the requests.
     *
     * @param request request to handle
     * @throws ShutdownException the request has to stop the thread
     * @throws COMPSsException Exception raised by user
     */
    public abstract void handleRequest(T request) throws ShutdownException, COMPSsException;


    private static class RequestQueue<T> {

        private final Queue<T> regularQueue = new LinkedList<>();
        private final Queue<T> priorityQueue = new LinkedList<>();
        private final Semaphore available = new Semaphore(0);


        public synchronized boolean offer(T request) {
            boolean b = regularQueue.add(request);
            available.release();
            return b;
        }

        public synchronized boolean offerWithPriority(T request) {
            boolean b = priorityQueue.add(request);
            available.release();
            return b;
        }

        public T take() throws InterruptedException {
            available.acquire();
            synchronized (this) {
                if (priorityQueue.isEmpty()) {
                    return regularQueue.poll();
                }
                return priorityQueue.poll();
            }
        }
    }
}
