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
package es.bsc.compss.worker;

import java.util.HashMap;
import java.util.Map;


public class COMPSsWorker {

    public static final String COMPSS_TASK_ID = "COMPSS_TASK_ID";

    private static final Map<Integer, CancelReason> TASKS_TO_CANCEL = new HashMap<>();


    /**
     * Add a cancellation point.
     * 
     * @throws Exception While waiting for the cancellation point.
     */
    public static final void cancellationPoint() throws Exception {
        String taskIdStr = System.getProperty(COMPSS_TASK_ID);
        if (taskIdStr != null) {
            CancelReason exceptionReason = TASKS_TO_CANCEL.get(Integer.parseInt(taskIdStr));
            if (exceptionReason != null) {
                // Treat exception
                switch (exceptionReason) {
                    case COMPSS_EXCEPTION:
                        // Print on the job console
                        System.out.println("Task " + taskIdStr + " cancelled because a COMPSs Exception occured.");
                        // Throw exception
                        throw new Exception("Task " + taskIdStr + " has been canceled.");
                    case TIMEOUT:
                        // Print on the job console
                        System.out.println("Task " + taskIdStr + " has timed out.");
                        // Throw exception
                        throw new Exception("Task " + taskIdStr + " timed out.");
                }
            }
        }
    }

    /**
     * Sets a task as cancelled.
     * 
     * @param taskId Task Id.
     */
    protected static final void setCancelled(int taskId, CancelReason reason) {
        TASKS_TO_CANCEL.put(taskId, reason);
    }
}
