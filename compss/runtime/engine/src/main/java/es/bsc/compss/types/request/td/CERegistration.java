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
package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.util.CoreManager;

import java.util.concurrent.Semaphore;

import es.bsc.compss.util.ResourceManager;

import java.util.LinkedList;


public class CERegistration extends TDRequest {

    private final CoreElementDefinition ced;
    private final Semaphore sem;

    /**
     * Creates a new CoreElement registration request
     *
     * @param ced
     * @param sem
     */
    public CERegistration(CoreElementDefinition ced, Semaphore sem) {
        this.ced = ced;
        this.sem = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the operation is done
     *
     * @return Semaphore where to synchronize until the operation is done
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    @Override
    public void process(TaskScheduler ts) {
        int coreId = CoreManager.registerNewCoreElement(ced);
        // Update the Resources structures
        LinkedList<Integer> newCores = new LinkedList<>();
        newCores.add(coreId);
        ResourceManager.coreElementUpdates(newCores);

        // Update the Scheduler structures
        ts.coreElementsUpdated();

        LOGGER.debug("Data structures resized and CE-resources links updated");
        sem.release();
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.CE_REGISTRATION;
    }

}
