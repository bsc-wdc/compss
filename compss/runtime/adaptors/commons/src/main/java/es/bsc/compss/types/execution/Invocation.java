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
package es.bsc.compss.types.execution;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.resources.ResourceDescription;
import java.util.List;


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
     * Returns the task language.
     *
     * @return The task language.
     */
    public Lang getLang();

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

    public List<Integer> getPredecessors();

    public Integer getNumSuccessors();

    public List<? extends InvocationParam> getParams();

    public InvocationParam getTarget();

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

    public OnFailure getOnFailure();

    public boolean producesEmptyResultsOnFailure();

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
