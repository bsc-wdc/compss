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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.tracing.TraceEvent;


public class OpenTaskGroupRequest implements APRequest {

    private String groupName;
    private Application app;


    /**
     * Request to open a task group.
     * 
     * @param groupName Name of the group.
     * @param app Application.
     */
    public OpenTaskGroupRequest(String groupName, Application app) {
        this.groupName = groupName;
        this.app = app;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        app.openTaskGroup(groupName);
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.WAIT_FOR_ALL_TASKS;
    }
}
