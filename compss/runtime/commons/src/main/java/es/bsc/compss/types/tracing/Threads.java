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
package es.bsc.compss.types.tracing;

public enum Threads {

    APP(1, "APP", "App thread", ExtraeTaskType.APPLICATION), //
    MAIN(1, "MAIN", "Main thread", ExtraeTaskType.RUNTIME), //
    AP(2, "RUNTIME AP", "Access Processor thread", ExtraeTaskType.RUNTIME), //
    TD(3, "RUNTIME TD", "Task Dispacher thread", ExtraeTaskType.RUNTIME), //
    FSL(4, "RUNTIME FS L", "File system thread", ExtraeTaskType.RUNTIME), //
    FSH(5, "RUNTIME FS H", "File system thread", ExtraeTaskType.RUNTIME), //
    TIMER(6, "RUNTIME TIMER", "Timer thread", ExtraeTaskType.RUNTIME), //
    WC(7, "RUNTIME WALLCLOCK", "WallClock thread", ExtraeTaskType.RUNTIME), //
    EXEC(8, "EXECUTOR", "Executor thread", ExtraeTaskType.EXECUTOR), //
    PYTHON_WORKER(9, "PYTHON WORKER", "Python worker", ExtraeTaskType.RUNTIME), //
    CACHE(10, "PYTHON OBJECT CACHE", "Python Cache manager", ExtraeTaskType.RUNTIME);


    public static enum ExtraeTaskType {

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
    public final String description;
    private final ExtraeTaskType task;


    private Threads(int id, String label, String description, ExtraeTaskType task) {
        this.id = id;
        this.label = label;
        this.description = description;
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
        if (id < 0 || id >= Threads.values().length) {
            return null;
        } else {
            if (id < 2) {
                return "";
            } else {
                return Threads.values()[id].label;

            }
        }
    }
}
