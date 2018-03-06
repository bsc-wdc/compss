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

/**
 * Abstract Representation of a listener for the job execution
 * 
 */
public interface JobListener {

    /**
     * Job status types
     *
     */
    public enum JobEndStatus {
        OK, // Success status
        TO_RESCHEDULE, // Task must be rescheduled
        TRANSFERS_FAILED, // Task transfers failed
        SUBMISSION_FAILED, // Task submission failed
        EXECUTION_FAILED; // Task execution failed
    }


    /**
     * Actions when job has successfully ended
     * 
     * @param job
     */
    public void jobCompleted(Job<?> job);

    /**
     * Actions when job has failed
     * 
     * @param job
     * @param endStatus
     */
    public void jobFailed(Job<?> job, JobEndStatus endStatus);

}
