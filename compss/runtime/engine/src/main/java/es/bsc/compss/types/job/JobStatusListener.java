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
package es.bsc.compss.types.job;

import es.bsc.compss.types.allocatableactions.ExecutionAction;


public class JobStatusListener implements JobListener {

    private final ExecutionAction execution;


    public JobStatusListener(ExecutionAction ex) {
        this.execution = ex;
    }

    @Override
    public void jobCompleted(Job<?> job) {
        // Mark execution as completed
        this.execution.completedJob(job);
    }

    @Override
    public void jobFailed(Job<?> job, JobEndStatus endStatus) {
        // Mark execution as failed
        this.execution.failedJob(job, endStatus);
    }

}
