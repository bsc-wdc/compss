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
package es.bsc.compss.types.request.ap;

import java.util.concurrent.Semaphore;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.location.DataLocation;


public class WaitForDataReadyToDeleteRequest extends APRequest {

    private final DataLocation loc;
    private final Semaphore sem;
    private final Semaphore semWait;
    private int nPermits;


    public WaitForDataReadyToDeleteRequest(DataLocation loc, Semaphore sem, Semaphore semWait) {
        this.loc = loc;
        this.sem = sem;
        this.semWait = semWait;
        this.nPermits = 0;
    }

    public DataLocation getLocation() {
        return loc;
    }

    public int getNumPermits() {
        return nPermits;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        LOGGER.info("[WaitForDataReadyToDelete] Notifying waiting data to DIP...");
        this.nPermits = dip.waitForDataReadyToDelete(loc, semWait);
        sem.release();
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.DELETE_FILE;
    }

}
