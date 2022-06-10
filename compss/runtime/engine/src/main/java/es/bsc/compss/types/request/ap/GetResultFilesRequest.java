/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.ResultFile;
import es.bsc.compss.types.data.operation.ResultListener;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Semaphore;


public class GetResultFilesRequest extends APRequest {

    private final Application app;
    private final Semaphore sem;

    private final LinkedList<ResultFile> blockedData;


    /**
     * Creates a new request to retrieve the result files.
     * 
     * @param app Application.
     * @param sem Waiting semaphore.
     */
    public GetResultFilesRequest(Application app, Semaphore sem) {
        this.app = app;
        this.sem = sem;
        this.blockedData = new LinkedList<>();
    }

    /**
     * Returns the application of the request.
     * 
     * @return The application of the request.
     */
    public Application getApp() {
        return this.app;
    }

    /**
     * Returns the waiting semaphore of the request.
     * 
     * @return The waiting semaphore of the request.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns a list containing the blocked result files.
     * 
     * @return A list containing the blocked result files.
     */
    public LinkedList<ResultFile> getBlockedData() {
        return this.blockedData;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ResultListener listener = new ResultListener(sem);
        Set<Integer> writtenDataIds = ta.getAndRemoveWrittenFiles(this.app);
        if (writtenDataIds != null) {
            for (int dataId : writtenDataIds) {
                ResultFile rf;
                rf = dip.blockDataAndGetResultFile(dataId, listener);
                if (rf == null) {
                    continue;
                }
                this.blockedData.add(rf);
            }
            listener.enable();
        } else {
            this.sem.release();
        }

    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.BLOCK_AND_GET_RESULT_FILES;
    }

}
