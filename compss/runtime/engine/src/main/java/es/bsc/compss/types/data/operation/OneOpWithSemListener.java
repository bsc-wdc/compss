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


public class OneOpWithSemListener extends EventListener {

    private static final Logger logger = LogManager.getLogger(Loggers.FTM_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    private Semaphore sem;


    public OneOpWithSemListener(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public void notifyEnd(DataOperation fOp) {
        sem.release();
    }

    @Override
    public void notifyFailure(DataOperation fOp, Exception e) {
        if (debug) {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed"
                + (fOp != null ? " on " + fOp.getName() : "") + ", file role is OPEN_FILE"
                + ", operation end state is FAILED", e);
        } else {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed"
                + (fOp != null ? " on " + fOp.getName() : "") + ", file role is OPEN_FILE"
                + ", operation end state is FAILED");
        }

        ErrorManager.warn("Open file tranfer " + (fOp != null ? " for " + fOp.getName() : "")
            + " failed. Check runtime.log for more details.");
        sem.release();
    }

}
