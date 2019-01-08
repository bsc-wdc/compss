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
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
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
        boolean hasTarget = taskParams.hasTargetObject();
        int numReturns = taskParams.getNumReturns();
        this.arguments = new LinkedList<>();
        this.results = new LinkedList<>();
        Parameter[] params = task.getParameters();
        int paramsCount = params.length;
        if (super.getLang().equals(Lang.PYTHON)) {
            if (hasTarget) {
                Parameter p = params[params.length - 1];
                target = new LocalParameter(p);
                paramsCount--;
            }
            for (int rIdx = 0; rIdx < numReturns; rIdx++) {
                Parameter p = params[params.length - (hasTarget ? 1 : 0) - numReturns + rIdx];
                results.addFirst(new LocalParameter(p));
            }
            paramsCount -= numReturns;
        } else {
            // Java or C/C++
            for (int rIdx = 0; rIdx < numReturns; rIdx++) {
                Parameter p = params[params.length - numReturns + rIdx];
                results.addFirst(new LocalParameter(p));
            }
            paramsCount -= numReturns;
            if (hasTarget) {
                Parameter p = params[params.length - numReturns - 1];
                target = new LocalParameter(p);
                paramsCount--;
            }
        }

        for (int paramIdx = 0; paramIdx < paramsCount; paramIdx++) {
            arguments.add(new LocalParameter(params[paramIdx]));
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
        MethodImplementation method = (MethodImplementation) this.impl;

        String className = method.getDeclaringClass();
        String methodName = taskParams.getName();

        return "LocalJob JobId" + this.jobId + " for method " + methodName + " at class " + className;
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return (MethodImplementation) this.impl;
    }

    @Override
    public boolean isDebugEnabled() {
        return debug;
    }

    @Override
    public List<? extends InvocationParam> getParams() {
        return arguments;
    }

    @Override
    public InvocationParam getTarget() {
        return target;
    }

    @Override
    public List<? extends InvocationParam> getResults() {
        return results;
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
