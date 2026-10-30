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
package es.bsc.compss.types.tracing;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.data.FileOpsManager;
import es.bsc.compss.execution.platform.ExecutionPlatform;

public enum RuntimeThreads {

    APP(1, "APP", ExtraeTaskType.APPLICATION), //
    MAIN(1, "MAIN", ExtraeTaskType.RUNTIME), //
    AP(AccessProcessor.COMPONENT_ID, "RUNTIME AP", ExtraeTaskType.RUNTIME), //
    TD(TaskDispatcher.COMPONENT_ID, "RUNTIME TD", ExtraeTaskType.RUNTIME), //
    FSL(FileOpsManager.LOW_PRIORITY_COMPONENT_ID, "RUNTIME FS L", ExtraeTaskType.RUNTIME), //
    FSH(FileOpsManager.HIGH_PRIORITY_COMPONENT_ID, "RUNTIME FS H", ExtraeTaskType.RUNTIME), //
    TIMER(ExecutionPlatform.EXECUTION_TIMER_COMPONENT_ID, "RUNTIME TIMER", ExtraeTaskType.RUNTIME), //
    EXEC(ExecutionPlatform.EXECUTOR_COMPONENT_ID, "EXECUTOR", ExtraeTaskType.EXECUTOR), //
    PYTHON_WORKER(9, "PYTHON WORKER", ExtraeTaskType.RUNTIME), //
    CACHE(10, "PYTHON OBJECT CACHE", ExtraeTaskType.RUNTIME);


    private static RuntimeThreads[] idToThread;

    static {
        int maxId = 0;
        for (RuntimeThreads t : RuntimeThreads.values()) {
            maxId = Math.max(maxId, t.id);
        }
        idToThread = new RuntimeThreads[maxId + 1];
        for (RuntimeThreads t : RuntimeThreads.values()) {
            idToThread[t.id] = t;
        }
    }


    public enum ExtraeTaskType {

        APPLICATION("1"), //
        RUNTIME("1"), //
        EXECUTOR("2"); //


        private final String label;


        private ExtraeTaskType(String label) {
            this.label = label;
        }

        public String getLabel() {
            return this.label;
        }
    }


    public final int id;
    public final String label;
    private final ExtraeTaskType task;


    RuntimeThreads(int id, String label, ExtraeTaskType task) {
        this.id = id;
        this.label = label;
        this.task = task;
    }

    public boolean isRuntime() {
        return this.task == ExtraeTaskType.RUNTIME;
    }

    /**
     * Return the label associated to the thread corresponding to the ID passed in as parameter. If the id is 0 or 1
     * 
     * @param id ID of the Thread whose label is requested
     * @return {@literal null}, if the thread Id is unrecognized; {@literal ""}, if the id is 0 or 1 (App or main
     *         thread); the label corresponding to the thread identified by the id passed in
     */
    public static String getLabelByID(int id) {
        if (id < 0 || id >= idToThread.length) {
            return null;
        } else {
            if (id < 2) {
                return "";
            } else {
                return idToThread[id].label;
            }
        }
    }
}
