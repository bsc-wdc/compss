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
package es.bsc.compss.executor;

import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.utils.ExecutionPlatformMirror;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;
import es.bsc.compss.types.resources.ResourceDescription;


public interface ExecutorContext {

    public ExecutionPlatformMirror getMirror(Class<?> invoker);

    public void registerMirror(Class<?> invoker, ExecutionPlatformMirror mirror);

    public int getSize();

    public Execution getJob();

    public InvocationResources acquireComputingUnits(int jobId, ResourceDescription requirements) throws UnsufficientAvailableComputingUnitsException;

    public void releaseComputingUnits(int jobId);
}
