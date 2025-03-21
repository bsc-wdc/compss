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
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.util.ErrorManager;

import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ResultListener extends EventListener {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.FTM_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private int operation = 0;
    private int errors = 0;
    private boolean enabled = false;

    private Semaphore sem;


    /**
     * New result listener.
     * 
     * @param sem Waiting semaphore.
     */
    public ResultListener(Semaphore sem) {
        this.sem = sem;
    }

    /**
     * Activates the Result listener.
     */
    public synchronized void enable() {
        this.enabled = true;
        if (this.operation == 0) {
            if (this.errors == 0) {
                doReady();
            } else {
                doFailures();
            }
        }
    }

    public synchronized void addOperation() {
        this.operation++;
    }

    @Override
    public synchronized void notifyEnd(DataOperation fOp) {
        this.operation--;
        if (this.operation == 0 && this.enabled) {
            if (this.errors == 0) {
                doReady();
            } else {
                doFailures();
            }
        }
    }

    @Override
    public synchronized void notifyFailure(DataOperation fOp, Exception e) {
        String fOpName = "N/A";
        if (fOp != null) {
            fOp.getName();
        }
        LOGGER.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOpName
            + ", file role is RESULT_FILE" + ", operation end state is FAILED", e);
        ErrorManager.warn("Result file tranfer " + fOpName + " failed. Check runtime.log for more details.");
        this.operation--;
        this.errors++;
        if (this.enabled && this.operation == 0) {
            doFailures();
        }
    }

    private void doReady() {
        this.sem.release();
    }

    private void doFailures() {
        this.sem.release();
    }

}
