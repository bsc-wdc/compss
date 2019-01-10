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
package es.bsc.compss.local;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.COMPSsMaster;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.MultiNodeImplementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.util.ErrorManager;

import java.util.LinkedList;
import java.util.List;


public class LocalJob extends Job<COMPSsMaster> implements Invocation {

    private final List<LocalParameter> arguments;
    private LocalParameter target;
    private LinkedList<LocalParameter> results;
    private MethodResourceDescription reqs;
    private final List<String> slaveWorkersNodeNames;


    public LocalJob(int taskId, TaskDescription task, Implementation impl, Resource res, List<String> slaveWorkersNodeNames,
            JobListener listener) {

        super(taskId, task, impl, res, listener);

        // Construct parameters
        boolean hasTarget = this.taskParams.hasTargetObject();
        int numReturns = this.taskParams.getNumReturns();
        this.arguments = new LinkedList<>();
        this.results = new LinkedList<>();
        Parameter[] params = task.getParameters();
        int paramsCount = params.length;
        if (super.getLang().equals(Lang.PYTHON)) {
            // Python parameters are in a different order
            if (hasTarget) {
                Parameter p = params[params.length - 1];
                this.target = new LocalParameter(p);
                paramsCount--;
            }
            for (int rIdx = 0; rIdx < numReturns; rIdx++) {
                Parameter p = params[params.length - (hasTarget ? 1 : 0) - numReturns + rIdx];
                this.results.addFirst(new LocalParameter(p));
            }
            paramsCount -= numReturns;
        } else {
            // Java or C/C++
            for (int rIdx = 0; rIdx < numReturns; rIdx++) {
                Parameter p = params[params.length - numReturns + rIdx];
                this.results.addFirst(new LocalParameter(p));
            }
            paramsCount -= numReturns;
            if (hasTarget) {
                Parameter p = params[params.length - numReturns - 1];
                this.target = new LocalParameter(p);
                paramsCount--;
            }
        }

        for (int paramIdx = 0; paramIdx < paramsCount; paramIdx++) {
            this.arguments.add(new LocalParameter(params[paramIdx]));
        }

        this.slaveWorkersNodeNames = slaveWorkersNodeNames;

        AbstractMethodImplementation absMethodImpl = (AbstractMethodImplementation) this.impl;
        this.reqs = absMethodImpl.getRequirements();
    }

    @Override
    public void submit() throws Exception {
        this.getResourceNode().runJob(this);
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public String getHostName() {
        return this.getResourceNode().getName();
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    @Override
    public String toString() {
        AbstractMethodImplementation absMethodImpl = (AbstractMethodImplementation) this.impl;
        String className;
        switch (absMethodImpl.getMethodType()) {
            case METHOD:
                MethodImplementation method = (MethodImplementation) this.impl;
                className = method.getDeclaringClass();
                break;
            case MULTI_NODE:
                MultiNodeImplementation multiNodeMethod = (MultiNodeImplementation) this.impl;
                className = multiNodeMethod.getDeclaringClass();
                break;
            default:
                ErrorManager.error("ERROR: Unrecognised methodtype " + absMethodImpl.getMethodType() + " on local adaptor");
                return null;
        }

        String methodName = this.taskParams.getName();
        return "LocalJob JobId" + this.jobId + " for method " + methodName + " at class " + className;
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        AbstractMethodImplementation absMethodImpl = (AbstractMethodImplementation) this.impl;
        switch (absMethodImpl.getMethodType()) {
            case METHOD:
                return (MethodImplementation) this.impl;
            case MULTI_NODE:
                return (MultiNodeImplementation) this.impl;
            default:
                ErrorManager.error("ERROR: Unrecognised methodtype " + absMethodImpl.getMethodType() + " on local adaptor");
                return null;
        }
    }

    @Override
    public boolean isDebugEnabled() {
        return DEBUG;
    }

    @Override
    public List<? extends InvocationParam> getParams() {
        return this.arguments;
    }

    @Override
    public InvocationParam getTarget() {
        return this.target;
    }

    @Override
    public List<? extends InvocationParam> getResults() {
        return this.results;
    }

    @Override
    public ResourceDescription getRequirements() {
        return this.reqs;
    }

    @Override
    public List<String> getSlaveNodesNames() {
        return this.slaveWorkersNodeNames;
    }

}
