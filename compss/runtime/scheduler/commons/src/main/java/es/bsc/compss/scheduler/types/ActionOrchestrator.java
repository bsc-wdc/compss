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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.worker.COMPSsException;


public interface ActionOrchestrator {

    /**
     * Notify that a given action is Running.
     * 
     * @param action Running action.
     */
    public void actionRunning(AllocatableAction action);

    /**
     * Notify that a given action is Completed.
     * 
     * @param action Completed action.
     */
    public void actionCompletion(AllocatableAction action);

    /**
     * Notify that a given action has failed.
     * 
     * @param action Failed action.
     */
    public void actionError(AllocatableAction action);

    /**
     * Notify that a given action has raised a COMPSs exception.
     * 
     * @param action Action which raised the exception.
     * @param e COMPSs action exception.
     */
    public void actionException(AllocatableAction action, COMPSsException e);

    /**
     * Notify that a given action should be upgraded because another action of the multinode group is waiting for a
     * resource.
     *
     * @param action Action which raised the exception.
     */
    public void actionUpgrade(AllocatableAction action);

}
