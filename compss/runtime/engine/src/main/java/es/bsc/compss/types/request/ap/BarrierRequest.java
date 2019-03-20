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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;

import java.util.concurrent.Semaphore;


public class BarrierRequest extends APRequest {

    private Semaphore sem;
    private Long appId;


    public BarrierRequest(Long appId, Semaphore sem) {
        this.appId = appId;
        this.sem = sem;
    }

    public Long getAppId() {
        return this.appId;
    }

    public Semaphore getSemaphore() {
        return this.sem;
    }

    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.barrier(this);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.WAIT_FOR_ALL_TASKS;
    }

}
