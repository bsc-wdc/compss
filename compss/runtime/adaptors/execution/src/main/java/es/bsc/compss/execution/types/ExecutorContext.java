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

package es.bsc.compss.execution.types;

import es.bsc.compss.executor.InvocationRunner;
import es.bsc.compss.executor.external.ExecutionPlatformMirror;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.types.execution.ExecutorRequest;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableResourcesException;
import es.bsc.compss.types.resources.ResourceDescription;

import java.util.Collection;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;


public interface ExecutorContext {

    public ExecutionPlatformMirror<?> getMirror(Class<?> invoker);

    public void registerMirror(Class<?> invoker, ExecutionPlatformMirror<?> mirror);

    public int getSize();

    public ExecutorRequest newThread();

    public ExecutorRequest getJob();

    public InvocationResources acquireResources(int jobId, ResourceDescription requirements,
        InvocationResources preferredAllocation) throws UnsufficientAvailableResourcesException;

    public void releaseResources(int jobId);

    public Collection<ExecutionPlatformMirror<?>> getMirrors();

    public void registerRunningJob(Invocation invocation, Invoker invoker, TimerTask timeoutHandler);

    public void blockedRunner(Invocation invocation, InvocationRunner executor, InvocationResources assignedResources);

    public void unblockedRunner(Invocation invocation, InvocationRunner executor,
        InvocationResources previousAllocation, Semaphore sem);

    public void unregisterRunningJob(int jobId);

}
