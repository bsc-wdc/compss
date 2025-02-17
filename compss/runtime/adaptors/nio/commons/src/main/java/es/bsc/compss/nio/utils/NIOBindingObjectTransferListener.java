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
package es.bsc.compss.nio.utils;

import es.bsc.comm.stage.Transfer;
import es.bsc.compss.nio.NIOAgent;

import java.util.concurrent.Semaphore;


public class NIOBindingObjectTransferListener {

    private int operation = 0;
    private int errors = 0;
    private boolean enabled = false;

    private final Semaphore sem;
    private NIOAgent agent;
    private Transfer transfer;


    /**
     * Creates a NIOBindingObjectTransferListener.
     * 
     * @param agent Associated NIOAgent.
     * @param sem Waiting semaphore.
     */
    public NIOBindingObjectTransferListener(NIOAgent agent, Semaphore sem) {
        this.agent = agent;
        this.sem = sem;
    }

    /**
     * Enables the listener.
     */
    public void enable() {
        boolean finished;
        boolean failed;
        synchronized (this) {
            this.enabled = true;
            finished = (this.operation == 0);
            failed = (this.errors > 0);
        }
        if (finished) {
            if (failed) {
                doFailures();
            } else {
                doReady();
            }
        }
    }

    /**
     * Adds a new operation to the listener.
     */
    public synchronized void addOperation() {
        this.operation++;
    }

    /**
     * Notifies the end of the operations.
     */
    public void notifyEnd() {
        boolean enabled;
        boolean finished;
        boolean failed;
        synchronized (this) {
            this.operation--;
            finished = (this.operation == 0);
            failed = (this.errors > 0);
            enabled = this.enabled;
        }
        if (finished && enabled) {
            if (failed) {
                doFailures();
            } else {
                doReady();
            }
        }
    }

    /**
     * Notifies a failure in one of the internal operations.
     * 
     * @param e Failure exception.
     */
    public void notifyFailure(Exception e) {
        boolean enabled;
        boolean finished;
        synchronized (this) {
            this.errors++;
            this.operation--;
            finished = (this.operation == 0);
            enabled = this.enabled;
        }

        if (enabled && finished) {
            doFailures();
        }
    }

    private void doReady() {
        this.sem.release();
    }

    private void doFailures() {
        this.sem.release();
    }

    /**
     * Waits for the listener.
     */
    public void aquire() {
        try {
            this.sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the associated transfer.
     * 
     * @return The associated transfer.
     */
    public Transfer getTransfer() {
        return this.transfer;
    }

    /**
     * Sets a new associated transfer.
     * 
     * @param t The new associated transfer.
     */
    public void setTransfer(Transfer t) {
        this.transfer = t;
    }

    /**
     * Returns the associated NIOAgent.
     * 
     * @return The associated NIOAgent.
     */
    public NIOAgent getAgent() {
        return this.agent;
    }

}
