/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.agent.util;

import es.bsc.compss.agent.rest.types.RemoteJobListener;
import es.bsc.compss.agent.rest.types.TaskProfile;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.job.JobEndStatus;

import java.util.HashMap;


/**
 * Register of jobs currently dispatched to local/remote resources.
 */
public class RemoteJobsRegistry {

    protected static final HashMap<String, RemoteJobListener> REGISTERED_JOBS = new HashMap<>();


    /**
     * Registers a job running and its corresponding listener.
     *
     * @param jobId Id of the submitted job.
     * @param listener listener handling the job status updates.
     */
    public static synchronized void registerJobListener(String jobId, RemoteJobListener listener) {
        REGISTERED_JOBS.put(jobId, listener);
    }

    /**
     * Notifies the end of a job execution.
     *
     * @param jobId Id of the ended job.
     * @param endStatus end status of the job.
     * @param paramTypes array containing the Data types of all the parameters involved in the operation.
     * @param paramLocations location where to find the parameter value on the node/id on the persistent storage system.
     * @param profile Profiling information related to the job execution
     */
    public static synchronized void notifyJobEnd(String jobId, JobEndStatus endStatus, DataType[] paramTypes,
        String[] paramLocations, TaskProfile profile) {

        RemoteJobListener listener = REGISTERED_JOBS.remove(jobId);
        listener.finishedExecution(endStatus, paramTypes, paramLocations, profile);
    }
}
