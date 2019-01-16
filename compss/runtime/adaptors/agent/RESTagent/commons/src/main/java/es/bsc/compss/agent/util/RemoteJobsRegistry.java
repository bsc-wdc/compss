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
package es.bsc.compss.agent.util;

import es.bsc.compss.agent.rest.types.RemoteJobListener;
import es.bsc.compss.types.job.JobListener;
import java.util.HashMap;


public class RemoteJobsRegistry {

    public static final HashMap<String, RemoteJobListener> REGISTERED_JOBS = new HashMap<>();

    public static void registerJobListener(String jobId, RemoteJobListener o) {
        REGISTERED_JOBS.put(jobId, o);
    }

    public static void notifyJobEnd(String jobId, JobListener.JobEndStatus endStatus, String[] paramResults) {
        RemoteJobListener listener = REGISTERED_JOBS.remove(jobId);
        listener.finishedExecution(endStatus, paramResults);
    }
}
