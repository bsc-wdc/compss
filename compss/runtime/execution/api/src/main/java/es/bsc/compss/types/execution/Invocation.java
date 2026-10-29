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
package es.bsc.compss.types.execution;

import es.bsc.compss.semantics.task.FailurePolicy;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.resources.ResourceDescription;
import java.util.List;

/**
 * Description of a task execution request. An invocation carries all the information that the execution engine needs to
 * run a task on a worker: the job and task identifiers, the method implementation to execute, its parameters, target
 * and results, the resource requirements, and the failure management policy. The execution engine and the language
 * invokers provided by the plugins read the invocation description; the runtime reports execution progress through
 * {@link #executionStarts()} and {@link #executionEnds()}.
 */
public interface Invocation {

    /**
     * Returns the job Id.
     *
     * @return The job Id.
     */
    public int getJobId();

    /**
     * Returns the job history.
     *
     * @return The job history.
     */
    public JobHistory getHistory();

    /**
     * Returns the task Id.
     *
     * @return The task Id.
     */
    public int getTaskId();

    /**
     * Returns the task type.
     *
     * @return The task type.
     */
    public TaskType getTaskType();

    /**
     * Returns the method implementation type.
     *
     * @return The method implementation type.
     */
    public AbstractMethodImplementation getMethodImplementation();

    /**
     * Returns whether the worker debug is enabled or not.
     *
     * @return {@literal true} if the worker debug is enabled, {@literal false} otherwise.
     */
    public boolean isDebugEnabled();

    /**
     * Returns the identifiers of the tasks that must finish before the invocation can be executed.
     *
     * @return The predecessors task identifiers.
     */
    public List<Integer> getPredecessors();

    /**
     * Returns the number of tasks that depend on the completion of the invocation, or {@code null} when unknown.
     *
     * @return The number of successor tasks.
     */
    public Integer getNumSuccessors();

    /**
     * Returns the parameters of the invocation, excluding the target object.
     *
     * @return The invocation parameters.
     */
    public List<? extends InvocationParam> getParams();

    /**
     * Returns the target object of the invocation, or {@code null} when the method does not have a target.
     *
     * @return The invocation target parameter.
     */
    public InvocationParam getTarget();

    /**
     * Returns the parameters where the invocation results are stored.
     *
     * @return The invocation result parameters.
     */
    public List<? extends InvocationParam> getResults();

    /**
     * Returns the resource description needed for the task execution.
     *
     * @return The resource description needed for the task execution.
     */
    public ResourceDescription getRequirements();

    /**
     * Returns the slave workers node names.
     *
     * @return The slave workers node names.
     */
    public List<String> getSlaveNodesNames();

    /**
     * Returns the failure policy to apply when the invocation fails.
     *
     * @return The failure management policy.
     */
    public FailurePolicy getOnFailure();

    /**
     * Returns whether the invocation must produce empty results when it fails.
     *
     * @return {@literal true} if the results are set to empty values on failure, {@literal false} otherwise.
     */
    public boolean producesEmptyResultsOnFailure();

    /**
     * Returns the maximum time that the invocation is allowed to run, in seconds. A value of 0 means no timeout.
     *
     * @return The invocation timeout, in seconds.
     */
    public long getTimeOut();

    /**
     * Returns a string identifying the interface to use for detecting parallelism within the invocation.
     *
     * @return interface id
     */
    public String getParallelismSource();

    /**
     * Registers that the invocation execution has started.
     */
    public void executionStarts();

    /**
     * Registers that the invocation execution has finished.
     */
    public void executionEnds();

}
