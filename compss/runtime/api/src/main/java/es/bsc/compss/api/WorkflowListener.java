/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.api;

import es.bsc.compss.worker.COMPSsException;
import java.util.concurrent.Semaphore;

public interface WorkflowListener {

    /**
     * Invoked when workflow's main code reaches a synchronization point and can make no progress until further notice.
     */
    public void onSynchronization();

    /**
     * Invoked when the synchronization blocking the workflow has been resolved and its execution can resume.
     * 
     * @param sem element to notify when the runner is ready
     */
    public void onReadyToContinue(Semaphore sem);

    /**
     * Invoked when the workflow has raised a COMPSsException and has been canceled.
     *
     * @param e Exception raised during the workflow execution
     */
    public void onException(COMPSsException e);

    /**
     * Invoked when the workflow has been canceled.
     */
    public void onCancellation();

    /**
     * Invoked when the workflow completes its execution.
     */
    public void onCompletion();

    /**
     * Invoked when the workflow fails.
     */
    public void onFailure();

    /**
     * Returns the monitor for those tasks belonging to the application.
     * 
     * @return monitor for the tasks of the application
     */
    public TaskMonitor getTaskMonitor();
}
