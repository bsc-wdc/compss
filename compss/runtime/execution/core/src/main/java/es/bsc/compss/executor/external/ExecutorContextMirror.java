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
package es.bsc.compss.executor.external;

import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.external.piped.PipePair;

public interface ExecutorContextMirror<E> {

    /**
     * Adds an executor to the platform mirror.
     *
     * @param executor executor to be added
     * @return object used to interact with the mirrored executor
     */
    E registerExecutor(Executor executor);

    /**
     * Removes an executor from the platform mirror.
     * 
     * @param executor executor to remove
     */
    void unregisterExecutor(Executor executor);

    /**
     * Stops the platform mirror.
     */
    void stop();

    /**
     * Cancels a job.
     * 
     * @param pipe pipe through which to cancel the job
     */
    void cancelJob(PipePair pipe);

}
