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
package es.bsc.compss.types.execution;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.job.Job.JobHistory;
import es.bsc.compss.types.resources.ResourceDescription;
import java.util.List;


/**
 *
 * @author flordan
 */
public interface Invocation {

    public int getJobId();

    public JobHistory getHistory();

    public int getTaskId();

    public TaskType getTaskType();

    public Lang getLang();

    public AbstractMethodImplementation getMethodImplementation();

    public boolean isDebugEnabled();

    public List<? extends InvocationParam> getParams();

    public InvocationParam getTarget();

    public List<? extends InvocationParam> getResults();

    public ResourceDescription getRequirements();

    public List<String> getSlaveNodesNames();

}
