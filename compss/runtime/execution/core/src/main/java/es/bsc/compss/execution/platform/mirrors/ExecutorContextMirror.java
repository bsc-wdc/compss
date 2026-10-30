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
package es.bsc.compss.execution.platform.mirrors;

import es.bsc.compss.execution.executor.Executor;

/**
 * Mirror of an executor context: keeps the executors registered with the platform and hands out the handles that
 * represent them. The handle type {@literal <E>} is the mirror's executor representation; jobs are cancelled through
 * that same handle.
 *
 * @param <E> executor handle type managed by the mirror
 */
public interface ExecutorContextMirror<E> {

    /**
     * Adds an executor to the platform mirror.
     *
     * @param executor executor to be added
     * @return object used to interact with the mirrored executor
     */
    E registerExecutor(Executor executor);

    /**
     * Returns the mirrored executor that this mirror associates to the executor on the current thread. The mirror
     * caches the current thread's mirrored executor while it can run; a stale or absent one is re-registered through
     * this mirror.
     *
     * @param executor executor whose mirrored executor must be obtained
     * @return mirrored executor associated to the executor on the current thread
     */
    E getMirroredExecutor(Executor executor);

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
     * @param handle handle of the executor running the job to cancel
     */
    void cancelJob(E handle);

}
