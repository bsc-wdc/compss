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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.Barrier;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.worker.COMPSsException;

import java.util.concurrent.Semaphore;


public class BarrierRequest implements APRequest, Barrier {

    private final String barrierName;
    private final Application app;

    private COMPSsException exception;

    private boolean released;
    private final Semaphore sem;

    private int graphSource = Integer.MIN_VALUE;


    /**
     * Creates a new barrier request.
     *
     * @param app Application.
     */
    public BarrierRequest(Application app) {
        this(app, "Barrier");
    }

    /**
     * Creates a new barrier request.
     *
     * @param app Application.
     * @param name Name of the request
     */
    public BarrierRequest(Application app, String name) {
        this.app = app;
        this.exception = null;
        this.sem = new Semaphore(0);
        this.released = false;
        this.barrierName = name;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.WAIT_FOR_ALL_TASKS;
    }

    /**
     * Returns the application of the request.
     *
     * @return The application of the request.
     */
    public final Application getApp() {
        return this.app;
    }

    @Override
    public final void setException(COMPSsException exception) {
        this.exception = exception;
    }

    @Override
    public final COMPSsException getException() {
        return exception;
    }

    @Override
    public final void process(AccessProcessor ap, TaskDispatcher td) {
        handleBarrier();
        sem.release();
    }

    public void handleBarrier() {
        app.reachesBarrier(this);
    }

    @Override
    public final void release() {
        released = true;
        LOGGER.info(this.barrierName + " for app " + this.app.getId() + " released");
        sem.release();
    }

    /**
     * Waits for all tasks to complete releasing and recovering the resources if needed.
     * 
     * @throws COMPSsException User-code exception raised on any of the tasks for which the barrier awaits
     */
    public final void waitForCompletion() throws COMPSsException {
        // Wait for request processing
        sem.acquireUninterruptibly();

        // Release resources while barrier not resolved
        boolean stalled = false;
        if (!released) {
            LOGGER.info(this.barrierName + " for app " + this.app.getId() + " becomes stalled. Releasing resources");
            this.app.stalled();
            stalled = true;
        }

        // Wait for all tasks completion
        sem.acquireUninterruptibly();

        // Wait for app to have resources
        if (stalled) {
            app.readyToContinue(sem);
            sem.acquireUninterruptibly();
            LOGGER.info(this.barrierName + " for app " + this.app.getId() + " reacquired resources");
        }

        if (exception != null) {
            LOGGER.debug(this.barrierName + " raised a COMPSsException ( " + exception.getMessage() + ")");
            throw exception;
        }
    }

    @Override
    public int getGraphSource() {
        return this.graphSource;
    }

    @Override
    public void setGraphSource(int id) {
        this.graphSource = id;
    }

}
