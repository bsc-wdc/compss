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

package es.bsc.compss.execution.executor;

import es.bsc.compss.execution.Invocation;
import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.exceptions.UnsufficientAvailableResourcesException;
import es.bsc.compss.execution.executor.binding.ExternalExecutor;
import es.bsc.compss.execution.invokers.Invoker;
import es.bsc.compss.execution.platform.ExecutorRequest;
import es.bsc.compss.execution.platform.InvocationResources;
import es.bsc.compss.execution.platform.mirrors.ExecutorContextMirror;
import es.bsc.compss.execution.platform.mirrors.MirrorFactory;
import es.bsc.compss.types.resources.ResourceDescription;
import java.util.concurrent.Semaphore;

public interface ExecutorContext {

    /**
     * Returns the invocation context to whom the execution platform belongs to.
     * 
     * @return Computing Platform
     */
    InvocationContext getContext();

    ExecutorContextMirror<?> getMirror(Class<?> invoker);

    void registerMirror(Class<?> invoker, ExecutorContextMirror<?> mirror);

    /**
     * Returns the mirrored executor that mirrors of the given type associate to the executor running on the current
     * thread. The mirror is looked up in the platform's registry and created and registered with the given factory on
     * first use; the mirror caches the current thread's mirrored executor while it can run and re-registers a stale or
     * absent one through itself.
     *
     * @param <E> mirrored executor type handed out by the mirror
     * @param <M> mirror type associated to the mirrored executor type
     * @param executor executor whose mirrored executor must be obtained
     * @param mirrorClass mirror type managing the mirrored executors
     * @param mirrorFactory factory creating the platform's mirror on first use
     * @return mirrored executor associated to the executor on the current thread
     */
    <E extends ExternalExecutor, M extends ExecutorContextMirror<E>> E getMirroredExecutor(Executor executor,
        Class<M> mirrorClass, MirrorFactory<M> mirrorFactory);

    int getSize();

    ExecutorRequest newThread();

    ExecutorRequest getJob();

    InvocationResources acquireResources(int jobId, ResourceDescription requirements,
        InvocationResources preferredAllocation) throws UnsufficientAvailableResourcesException;

    void releaseResources(int jobId);

    void registerRunningJob(Invocation invocation, Invoker invoker);

    void blockedRunner(Invocation invocation, InvocationRunner executor, InvocationResources assignedResources);

    void unblockedRunner(Invocation invocation, InvocationRunner executor, InvocationResources previousAllocation,
        Semaphore sem);

    void unregisterRunningJob(int jobId);

    /**
     * Registers that an executor has finished.
     * 
     * @param executor executor finished
     */
    void finishedExecutor(Executor executor);
}
