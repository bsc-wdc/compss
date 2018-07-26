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
package es.bsc.compss.invokers.test.utils;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.job.Job.JobHistory;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import java.util.LinkedList;
import java.util.List;


/**
 *
 * @author flordan
 */
public class FakeInvocation implements Invocation {

    private final int jobId;
    private final int taskId;
    private final TaskType type;
    private final Lang lang;
    private final AbstractMethodImplementation impl;
    private final ResourceDescription requirements;
    private final List<InvocationParam> params;
    private final InvocationParam target;
    private final List<InvocationParam> results;
    private final List<String> slaves;
    private final JobHistory history;

    private FakeInvocation(
            int jobId,
            int taskId,
            TaskType type,
            Lang lang,
            AbstractMethodImplementation impl,
            ResourceDescription requirements,
            List<InvocationParam> params,
            InvocationParam target,
            List<InvocationParam> results,
            List<String> slaves,
            JobHistory history
    ) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.type = type;
        this.lang = lang;
        this.impl = impl;
        this.requirements = requirements;
        this.params = params;
        this.target = target;
        this.results = results;
        this.slaves = slaves;
        this.history = history;
    }

    @Override
    public int getJobId() {
        return this.jobId;
    }

    @Override
    public int getTaskId() {
        return this.taskId;
    }

    @Override
    public TaskType getTaskType() {
        return this.type;
    }

    @Override
    public Lang getLang() {
        return this.lang;
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
    }

    @Override
    public List<InvocationParam> getParams() {
        return this.params;
    }

    @Override
    public InvocationParam getTarget() {
        return target;
    }

    @Override
    public List<InvocationParam> getResults() {
        return this.results;
    }

    @Override
    public ResourceDescription getRequirements() {
        return requirements;
    }

    @Override
    public List<String> getSlaveNodesNames() {
        return slaves;
    }

    @Override
    public JobHistory getHistory() {
        return history;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }


    public static class Builder {

        final FakeInvocation inv;

        public Builder() {
            inv = new FakeInvocation(
                    0,
                    0,
                    TaskType.METHOD,
                    Lang.JAVA,
                    null,
                    new MethodResourceDescription(),
                    new LinkedList<>(),
                    null,
                    new LinkedList<>(),
                    new LinkedList<>(),
                    JobHistory.NEW
            );
        }

        private Builder(FakeInvocation inv) {
            this.inv = inv;
        }

        public Builder setJobId(int jobId) {
            return new Builder(
                    new FakeInvocation(
                            jobId,
                            this.inv.taskId,
                            this.inv.type,
                            this.inv.lang,
                            this.inv.impl,
                            this.inv.requirements,
                            this.inv.params,
                            this.inv.target,
                            this.inv.results,
                            this.inv.slaves,
                            this.inv.history
                    ));
        }

        public Builder setTaskId(int taskId) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            taskId,
                            this.inv.type,
                            this.inv.lang,
                            this.inv.impl,
                            this.inv.requirements,
                            this.inv.params,
                            this.inv.target,
                            this.inv.results,
                            this.inv.slaves,
                            this.inv.history
                    ));
        }

        public Builder setType(TaskType type) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            this.inv.taskId,
                            type,
                            this.inv.lang,
                            this.inv.impl,
                            this.inv.requirements,
                            this.inv.params,
                            this.inv.target,
                            this.inv.results,
                            this.inv.slaves,
                            this.inv.history
                    ));
        }

        public Builder setLang(Lang lang) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            this.inv.taskId,
                            this.inv.type,
                            lang,
                            this.inv.impl,
                            this.inv.requirements,
                            this.inv.params,
                            this.inv.target,
                            this.inv.results,
                            this.inv.slaves,
                            this.inv.history
                    ));
        }

        public Builder setImpl(AbstractMethodImplementation impl) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            this.inv.taskId,
                            this.inv.type,
                            this.inv.lang,
                            impl,
                            this.inv.requirements,
                            this.inv.params,
                            this.inv.target,
                            this.inv.results,
                            this.inv.slaves,
                            this.inv.history
                    ));
        }

        public Builder setRequirements(ResourceDescription requirements) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            this.inv.taskId,
                            this.inv.type,
                            this.inv.lang,
                            this.inv.impl,
                            requirements,
                            this.inv.params,
                            this.inv.target,
                            this.inv.results,
                            this.inv.slaves,
                            this.inv.history
                    ));
        }

        public Builder setParams(List<InvocationParam> params) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            this.inv.taskId,
                            this.inv.type,
                            this.inv.lang,
                            this.inv.impl,
                            this.inv.requirements,
                            params,
                            this.inv.target,
                            this.inv.results,
                            this.inv.slaves,
                            this.inv.history
                    ));
        }

        public Builder setTarget(InvocationParam target) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            this.inv.taskId,
                            this.inv.type,
                            this.inv.lang,
                            this.inv.impl,
                            this.inv.requirements,
                            this.inv.params,
                            target,
                            this.inv.results,
                            this.inv.slaves,
                            this.inv.history
                    ));
        }

        public Builder setResult(List<InvocationParam> results) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            this.inv.taskId,
                            this.inv.type,
                            this.inv.lang,
                            this.inv.impl,
                            this.inv.requirements,
                            this.inv.params,
                            this.inv.target,
                            results,
                            this.inv.slaves,
                            this.inv.history
                    ));
        }

        public Builder setSlaves(List<String> slaves) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            this.inv.taskId,
                            this.inv.type,
                            this.inv.lang,
                            this.inv.impl,
                            this.inv.requirements,
                            this.inv.params,
                            this.inv.target,
                            this.inv.results,
                            slaves,
                            this.inv.history
                    ));
        }

        public Builder setResult(JobHistory history) {
            return new Builder(
                    new FakeInvocation(
                            this.inv.jobId,
                            this.inv.taskId,
                            this.inv.type,
                            this.inv.lang,
                            this.inv.impl,
                            this.inv.requirements,
                            this.inv.params,
                            this.inv.target,
                            this.inv.results,
                            this.inv.slaves,
                            history
                    ));
        }

        public FakeInvocation build() {
            return inv;
        }
    }

}
