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
package es.bsc.compss.worker;

import java.util.HashMap;

public class COMPSsWorker {
    public static final String COMPSS_TASK_ID = "COMPSS_TASK_ID";
    private static HashMap<Integer, Boolean> tasksToCancel;
    
    
    public static void cancellationPoint() throws Exception {
        String taskIdStr = System.getProperty(COMPSS_TASK_ID);
        if (taskIdStr != null && tasksToCancel !=null) {
            Boolean toCancel = tasksToCancel.get(Integer.parseInt(taskIdStr));
            if (toCancel!=null && toCancel) {
                throw new Exception("Task " + taskIdStr + " has been cancelled.");
            }
        }
    }
    
    protected final static void setCancelled(int taskId) {
        if(tasksToCancel == null) {
            tasksToCancel = new HashMap<>();
        }
        tasksToCancel.put(taskId, true);
    }
}
