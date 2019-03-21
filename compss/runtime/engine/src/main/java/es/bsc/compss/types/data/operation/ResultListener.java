/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Semaphore;


public class ResultListener extends EventListener {

    private int operation = 0;
    private int errors = 0;
    private boolean enabled = false;

    private static final Logger logger = LogManager.getLogger(Loggers.FTM_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    private Semaphore sem;


    public ResultListener(Semaphore sem) {
        this.sem = sem;
    }

    public synchronized void enable() {
        enabled = true;
        if (operation == 0) {
            if (errors == 0) {
                doReady();
            } else {
                doFailures();
            }
        }
    }

    public synchronized void addOperation() {
        operation++;
    }

    @Override
    public synchronized void notifyEnd(DataOperation fOp) {
        operation--;
        if (operation == 0 && enabled) {
            if (errors == 0) {
                doReady();
            } else {
                doFailures();
            }
        }
    }

    @Override
    public synchronized void notifyFailure(DataOperation fOp, Exception e) {
        if (debug) {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is RESULT_FILE" + ", operation end state is FAILED", e);
        } else {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is RESULT_FILE" + ", operation end state is FAILED");
        }
        operation--;
        errors++;
        if (enabled && operation == 0) {
            doFailures();
        }
    }

    private void doReady() {
        sem.release();
    }

    private void doFailures() {
        sem.release();
    }

}
