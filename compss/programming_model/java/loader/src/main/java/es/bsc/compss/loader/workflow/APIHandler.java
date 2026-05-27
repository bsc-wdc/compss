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

package es.bsc.compss.loader.workflow;

import es.bsc.compss.loader.workflow.data.StreamRegistry;
import es.bsc.compss.worker.COMPSsException;

public final class APIHandler {

    /**
     * Barrier.
     *
     * @param wf Workflow invoking the API
     */
    public static void barrier(JavaWorkflow wf) {
        wf.barrier();
    }

    /**
     * Barrier with noMoreTasks flag to avoid file transfers.
     *
     * @param wf Workflow invoking the API
     * @param noMoreTasks Whether there are more tasks to be created or not.
     */
    public static void barrier(JavaWorkflow wf, boolean noMoreTasks) {
        wf.barrier(noMoreTasks);
    }

    /**
     * Barrier for a group of tasks.
     *
     * @param wf Workflow invoking the API
     * @param groupName Name of the group to perform the barrier.
     */
    public static void barrierGroup(JavaWorkflow wf, String groupName) throws COMPSsException {
        wf.barrierGroup(groupName);
    }

    /**
     * Cancel for a group of tasks.
     *
     * @param wf Workflow invoking the API
     * @param groupName Name of the group to cancel.
     */
    public static void cancelGroup(JavaWorkflow wf, String groupName) throws COMPSsException {
        wf.cancelTaskGroup(groupName);
    }

    /**
     * Unregister the given object from the Runtime.
     *
     * @param wf Workflow invoking the API
     * @param o Object to unregister.
     */
    public static void deregisterObject(JavaWorkflow wf, Object o) {
        wf.removeObject(o);
    }

    /**
     * Returns the file specified by the given abstract pathname.
     *
     * @param wf Workflow invoking the API
     * @param fileName File path.
     */
    public static void getFile(JavaWorkflow wf, String fileName) {
        wf.getFile(fileName);
        StreamRegistry.deleteTaskFile(fileName);
    }

    /**
     * Returns the file specified by the given abstract pathname.
     *
     * @param wf Workflow invoking the API
     * @param path Directory path.
     */
    public static void getDirectory(JavaWorkflow wf, String path) {
        wf.getDirectory(path);
        StreamRegistry.deleteTaskFile(path);
    }

    /**
     * Requests a checkpoint of the tasks and data.
     *
     * @param wf Workflow invoking the API
     */
    public static void snapshot(JavaWorkflow wf) {
        wf.snapshot();
    }

}
