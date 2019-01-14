/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.agent.rest.types;


public class TaskProfile {

    private final long taskReception = System.currentTimeMillis();
    private Long taskCreation = null;
    private Long taskAnalyzed = null;
    private Long taskScheduled = null;
    private Long executionStart = null;
    private Long executionEnd = null;
    private Long taskEnd = null;

    public TaskProfile() {

    }

    public void created() {
        taskCreation = System.currentTimeMillis();
    }

    public void end() {
        taskEnd = System.currentTimeMillis();
    }

    public Long getTotalTime() {
        Long length = null;
        if (taskEnd != null) {
            length = taskEnd - taskReception;
        }
        return length;
    }

    public void finished() {
        executionEnd = System.currentTimeMillis();
    }

    public void submitted() {
        executionStart = System.currentTimeMillis();
    }

    public void scheduled() {
        taskScheduled = System.currentTimeMillis();
    }

    public void processedAccesses() {
        taskAnalyzed = System.currentTimeMillis();
    }
}
