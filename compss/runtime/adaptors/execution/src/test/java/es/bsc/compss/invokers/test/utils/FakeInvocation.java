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
package es.bsc.compss.invokers.test.utils;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import java.util.LinkedList;
import java.util.List;


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
    private final long[] profile;


    private FakeInvocation(int jobId, int taskId, TaskType type, Lang lang, AbstractMethodImplementation impl,
        ResourceDescription requirements, List<InvocationParam> params, InvocationParam target,
        List<InvocationParam> results, List<String> slaves, JobHistory history) {

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
        this.profile = new long[2];
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
    public OnFailure getOnFailure() {
        return OnFailure.RETRY;
    }

    @Override
    public boolean producesEmptyResultsOnFailure() {
        return false;
    }

    @Override
    public long getTimeOut() {
        return 0L;
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

    @Override
    public List<Integer> getPredecessors() {
        return null;
    }

    @Override
    public Integer getNumSuccessors() {
        return null;
    }

    @Override
    public String getParallelismSource() {
        return null;
    }

    public long[] getProfileTimes() {
        return this.profile;
    }

    @Override
    public void executionStarts() {
        this.profile[0] = System.currentTimeMillis();
    }

    @Override
    public void executionEnds() {
        this.profile[1] = System.currentTimeMillis();
    }


    public static class Builder {

        final FakeInvocation inv;


        public Builder() {
            inv = new FakeInvocation(0, 0, TaskType.METHOD, Lang.JAVA, null, new MethodResourceDescription(),
                new LinkedList<>(), null, new LinkedList<>(), new LinkedList<>(), JobHistory.NEW);
        }

        private Builder(FakeInvocation inv) {
            this.inv = inv;
        }

        /**
         * Set Job Id.
         *
         * @param jobId Job Id
         * @return
         */
        public Builder setJobId(int jobId) {
            return new Builder(new FakeInvocation(jobId, this.inv.taskId, this.inv.type, this.inv.lang, this.inv.impl,
                this.inv.requirements, this.inv.params, this.inv.target, this.inv.results, this.inv.slaves,
                this.inv.history));
        }

        /**
         * Set task Id.
         *
         * @param taskId Task Id.
         * @return
         */
        public Builder setTaskId(int taskId) {
            return new Builder(new FakeInvocation(this.inv.jobId, taskId, this.inv.type, this.inv.lang, this.inv.impl,
                this.inv.requirements, this.inv.params, this.inv.target, this.inv.results, this.inv.slaves,
                this.inv.history));
        }

        /**
         * Set type.
         *
         * @param type Type
         * @return
         */
        public Builder setType(TaskType type) {
            return new Builder(new FakeInvocation(this.inv.jobId, this.inv.taskId, type, this.inv.lang, this.inv.impl,
                this.inv.requirements, this.inv.params, this.inv.target, this.inv.results, this.inv.slaves,
                this.inv.history));
        }

        /**
         * Set Language.
         *
         * @param lang Lang
         * @return
         */
        public Builder setLang(Lang lang) {
            return new Builder(new FakeInvocation(this.inv.jobId, this.inv.taskId, this.inv.type, lang, this.inv.impl,
                this.inv.requirements, this.inv.params, this.inv.target, this.inv.results, this.inv.slaves,
                this.inv.history));
        }

        /**
         * Set Implementation.
         *
         * @param impl Implementation
         * @return
         */
        public Builder setImpl(AbstractMethodImplementation impl) {
            return new Builder(new FakeInvocation(this.inv.jobId, this.inv.taskId, this.inv.type, this.inv.lang, impl,
                this.inv.requirements, this.inv.params, this.inv.target, this.inv.results, this.inv.slaves,
                this.inv.history));
        }

        /**
         * Set Requirements.
         *
         * @param requirements Requirements
         * @return
         */
        public Builder setRequirements(ResourceDescription requirements) {
            return new Builder(new FakeInvocation(this.inv.jobId, this.inv.taskId, this.inv.type, this.inv.lang,
                this.inv.impl, requirements, this.inv.params, this.inv.target, this.inv.results, this.inv.slaves,
                this.inv.history));
        }

        /**
         * Set parameters.
         *
         * @param params Parameters
         * @return
         */
        public Builder setParams(List<InvocationParam> params) {
            return new Builder(new FakeInvocation(this.inv.jobId, this.inv.taskId, this.inv.type, this.inv.lang,
                this.inv.impl, this.inv.requirements, params, this.inv.target, this.inv.results, this.inv.slaves,
                this.inv.history));
        }

        /**
         * Set Target.
         *
         * @param target Target parameter
         * @return
         */
        public Builder setTarget(InvocationParam target) {
            return new Builder(new FakeInvocation(this.inv.jobId, this.inv.taskId, this.inv.type, this.inv.lang,
                this.inv.impl, this.inv.requirements, this.inv.params, target, this.inv.results, this.inv.slaves,
                this.inv.history));
        }

        /**
         * Set result.
         *
         * @param results Resuts arameters
         * @return
         */
        public Builder setResult(List<InvocationParam> results) {
            return new Builder(new FakeInvocation(this.inv.jobId, this.inv.taskId, this.inv.type, this.inv.lang,
                this.inv.impl, this.inv.requirements, this.inv.params, this.inv.target, results, this.inv.slaves,
                this.inv.history));
        }

        /**
         * Set Result.
         *
         * @param history job history
         * @return
         */
        public Builder setResult(JobHistory history) {
            return new Builder(new FakeInvocation(this.inv.jobId, this.inv.taskId, this.inv.type, this.inv.lang,
                this.inv.impl, this.inv.requirements, this.inv.params, this.inv.target, this.inv.results,
                this.inv.slaves, history));
        }

        /**
         * Set slaves.
         *
         * @param slaves Slave nodes
         * @return
         */
        public Builder setSlaves(List<String> slaves) {
            return new Builder(new FakeInvocation(this.inv.jobId, this.inv.taskId, this.inv.type, this.inv.lang,
                this.inv.impl, this.inv.requirements, this.inv.params, this.inv.target, this.inv.results, slaves,
                this.inv.history));
        }

        public FakeInvocation build() {
            return inv;
        }
    }

}
