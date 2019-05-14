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

import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Task;


public class TaskAnalysisRequest extends APRequest {

    private final Task task;


    /**
     * Creates a new request to analyze a task.
     * 
     * @param task Task to analyze.
     */
    public TaskAnalysisRequest(Task task) {
        this.task = task;
    }

    /**
     * Returns the task to analyze.
     * 
     * @return The task to analyze.
     */
    public Task getTask() {
        return this.task;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.processTask(this.task);
        td.executeTask(ap, this.task);
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onAccessesProcessed();
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.ANALYSE_TASK;
    }

}
