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

package es.bsc.compss.loader.total;

import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.loader.JavaWorkflow;
import es.bsc.compss.worker.COMPSsException;


public final class APIHandler {

    /**
     * Barrier.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     */
    public static void barrier(COMPSsRuntime api, JavaWorkflow wf) {
        wf.barrier();
    }

    /**
     * Barrier with noMoreTasks flag to avoid file transfers.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     * @param noMoreTasks Whether there are more tasks to be created or not.
     */
    public static void barrier(COMPSsRuntime api, JavaWorkflow wf, boolean noMoreTasks) {
        wf.barrier(noMoreTasks);
    }

    /**
     * Barrier for a group of tasks.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     * @param groupName Name of the group to perform the barrier.
     */
    public static void barrierGroup(COMPSsRuntime api, JavaWorkflow wf, String groupName) throws COMPSsException {
        wf.barrierGroup(groupName);
    }

    /**
     * Cancel for a group of tasks.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     * @param groupName Name of the group to cancel.
     */
    public static void cancelGroup(COMPSsRuntime api, JavaWorkflow wf, String groupName) throws COMPSsException {
        wf.cancelTaskGroup(groupName);
    }

    /**
     * Unregister the given object from the Runtime.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     * @param o Object to unregister.
     */
    public static void deregisterObject(COMPSsRuntime api, JavaWorkflow wf, Object o) {
        wf.removeObject(o);
    }

    /**
     * Returns the file specified by the given abstract pathname.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     * @param fileName File path.
     */
    public static void getFile(COMPSsRuntime api, JavaWorkflow wf, String fileName) {
        api.getFile(wf.getId(), fileName);
    }

    /**
     * Returns the number of active resources.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     * @return The number of active resources.
     */
    public static int getNumberOfResources(COMPSsRuntime api, JavaWorkflow wf) {
        return api.getNumberOfResources();
    }

    /**
     * Requests the creation of {@code numResources} resources.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     * @param numResources Number of resources to create.
     * @param groupName name of the group to cancel if the creation fails
     */
    public static void requestResources(COMPSsRuntime api, JavaWorkflow wf, int numResources, String groupName) {
        api.requestResources(wf.getId(), numResources, groupName);
    }

    /**
     * Requests the destruction of {@code numResources} resources.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     * @param numResources Number of resources to destroy.
     */
    public static void freeResources(COMPSsRuntime api, JavaWorkflow wf, int numResources, String groupName) {
        api.freeResources(wf.getId(), numResources, groupName);
    }

    /**
     * Requests a checkpoint of the tasks and data.
     *
     * @param api COMPSsRuntimeAPI
     * @param wf Workflow invoking the API
     */
    public static void snapshot(COMPSsRuntime api, JavaWorkflow wf) {
        wf.snapshot();
    }

}
