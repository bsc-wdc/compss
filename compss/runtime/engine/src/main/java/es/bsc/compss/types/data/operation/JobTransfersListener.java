/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.types.allocatableactions.ExecutionAction;
import es.bsc.compss.types.data.listener.EventListener;


public class JobTransfersListener extends EventListener {

    private int operation = 0;
    private int errors = 0;
    private boolean enabled = false;

    private static final Logger LOGGER = LogManager.getLogger(Loggers.FTM_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private final ExecutionAction execution;


    public JobTransfersListener(ExecutionAction execution) {
        this.execution = execution;
    }

    public void enable() {
        boolean finished;
        boolean failed;
        synchronized (this) {
            enabled = true;
            finished = (operation == 0);
            failed = (errors > 0);
        }

        if (finished) {
            if (failed) {
                doFailures();
            } else {
                doReady();
            }
        }
    }

    public synchronized void addOperation() {
        operation++;
    }

    @Override
    public void notifyEnd(DataOperation fOp) {
        boolean enabled;
        boolean finished;
        boolean failed;
        synchronized (this) {
            operation--;
            finished = operation == 0;
            failed = errors > 0;
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

    @Override
    public void notifyFailure(DataOperation fOp, Exception e) {
        if (DEBUG) {
            LOGGER.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is JOB_FILE, operation end state is FAILED", e);
        } else {
            LOGGER.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is JOB_FILE operation end state is FAILED");
        }

        boolean enabled;
        boolean finished;
        synchronized (this) {
            errors++;
            operation--;
            finished = operation == 0;
            enabled = this.enabled;
        }
        if (enabled && finished) {
            doFailures();
        }
    }

    private void doReady() {
        execution.doSubmit(this.getId());
    }

    private void doFailures() {
        execution.failedTransfers(errors);
    }

}
