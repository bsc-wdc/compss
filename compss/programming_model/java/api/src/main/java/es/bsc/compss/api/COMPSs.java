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
package es.bsc.compss.api;

import es.bsc.compss.worker.COMPSsException;


/**
 * COMPSs API Class for JAVA.
 */
public class COMPSs {

    private static final String SKIP_MESSAGE = "COMPSs Runtime is not loaded. Skipping call";


    /**
     * Barrier.
     */
    public static void barrier() {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

    /**
     * Barrier with noMoreTasks flag to avoid file transfers.
     * 
     * @param noMoreTasks Whether there are more tasks to be created or not.
     */
    public static void barrier(boolean noMoreTasks) {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

    /**
     * Barrier for a group of tasks.
     * 
     * @param groupName Name of the group to perform the barrier.
     */
    public static void barrierGroup(String groupName) throws COMPSsException {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

    /**
     * Cancel for a group of tasks.
     * 
     * @param groupName Name of the group to cancel.
     */
    public static void cancelGroup(String groupName) throws COMPSsException {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

    /**
     * Unregister the given object from the Runtime.
     * 
     * @param o Object to unregister.
     */
    public static void deregisterObject(Object o) {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

    /**
     * Returns the file specified by the given abstract pathname.
     * 
     * @param fileName File path.
     */
    public static void getFile(String fileName) {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

    /**
     * Returns the number of active resources.
     * 
     * @return The number of active resources.
     */
    public int getNumberOfResources() {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
        return 0;
    }

    /**
     * Requests the creation of {@code numResources} resources.
     * 
     * @param numResources Number of resources to create.
     */
    public void requestResources(int numResources) {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

    /**
     * Requests the destruction of {@code numResources} resources.
     * 
     * @param numResources Number of resources to destroy.
     */
    public void freeResources(int numResources) {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

    /**
     * Requests a checkpoint of the tasks and data.
     */
    public static void snapshot() {
        System.out.println(SKIP_MESSAGE);
    }

}
