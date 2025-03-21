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
package es.bsc.compss.types.job;

import es.bsc.compss.types.COMPSsWorker;


/**
 * Abstract representation of a job.
 *
 * @param <T> COMPSs Worker
 */
public interface Job<T extends COMPSsWorker> {

    /**
     * Returns the job id.
     *
     * @return
     */
    public int getJobId();

    /**
     * Returns the id of the task executed by this job.
     *
     * @return
     */
    public int getTaskId();

    /*
     * -------------------------------------------------------------------------------------------------
     * ---------------------------------- LIFECYCLE MANAGEMENT -----------------------------------------
     * -------------------------------------------------------------------------------------------------
     */

    /**
     * Orders the copying of all the necessary input data.
     */
    public void stageIn();

    /**
     * Orders the execution of the task on the resource.
     */
    public void submit();

    /**
     * Stops the job.
     *
     * @throws Exception Error when stopping a job
     */
    public void cancel() throws Exception;

}
