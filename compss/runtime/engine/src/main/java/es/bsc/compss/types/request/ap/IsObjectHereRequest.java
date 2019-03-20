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
import es.bsc.compss.types.data.DataInstanceId;

import java.util.concurrent.Semaphore;


public class IsObjectHereRequest extends APRequest {

    private int code;
    private Semaphore sem;

    private boolean response;


    public IsObjectHereRequest(int code, Semaphore sem) {
        this.code = code;
        this.sem = sem;
    }

    public int getdCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public Semaphore getSemaphore() {
        return sem;
    }

    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    public boolean getResponse() {
        return response;
    }

    public void setResponse(boolean response) {
        this.response = response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        DataInstanceId dId = dip.getLastDataAccess(code);
        response = dip.isHere(dId);
        sem.release();
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.IS_OBJECT_HERE;
    }

}
