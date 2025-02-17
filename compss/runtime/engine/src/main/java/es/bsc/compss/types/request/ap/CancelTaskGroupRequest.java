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
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskGroup;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.request.listener.StaticMultioperationSemaphore;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.worker.COMPSsException;
import java.util.List;
import java.util.concurrent.Semaphore;


public class CancelTaskGroupRequest implements APRequest {

    private final Application app;
    private final String groupName;
    private final Semaphore sem;


    /**
     * Creates a request to cancel all tasks of an application.
     * 
     * @param app Application.
     * @param groupName Task group name to cancel.
     * @param sem Synchronising semaphore.
     */
    public CancelTaskGroupRequest(Application app, String groupName, Semaphore sem) {
        this.app = app;
        this.groupName = groupName;
        this.sem = sem;
    }

    /**
     * Returns the waiting semaphore.
     *
     * @return The waiting semaphore.
     */
    public final Semaphore getSemaphore() {
        return this.sem;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.CANCEL_TASK_GROUP;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) throws ShutdownException, COMPSsException {
        LOGGER.debug("Cancelling tasks of group " + groupName);
        cancelGroup(td);
    }

    protected final void cancelGroup(TaskDispatcher td) {
        TaskGroup tg = app.removeGroup(groupName);
        if (tg != null) {
            List<Task> tasks = tg.getTasks();
            int taskCount = tasks.size();
            StaticMultioperationSemaphore listener = new StaticMultioperationSemaphore(taskCount, sem);
            LOGGER.debug("Cancelling " + taskCount + " tasks.");
            for (Task t : tasks) {
                td.cancelTasks(t, listener);
            }
        } else {
            sem.release();
        }
    }

    /**
     * Returns the associated application.
     * 
     * @return The associated application.
     */
    public Application getApp() {
        return this.app;
    }

    /**
     * Returns the associated task group name.
     *
     * @return The associated task group name.
     */
    public final String getGroupName() {
        return this.groupName;
    }
}
