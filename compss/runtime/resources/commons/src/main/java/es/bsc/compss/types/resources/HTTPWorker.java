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
package es.bsc.compss.types.resources;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.configuration.HTTPConfiguration;


public class HTTPWorker extends Worker<HTTPResourceDescription> {

    public HTTPWorker(String httpWorker, HTTPResourceDescription description, HTTPConfiguration config) {
        super(httpWorker, description, config, null);
    }

    public HTTPWorker(HTTPWorker httpWorker) {
        super(httpWorker);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.HTTP;
    }

    @Override
    public boolean canRun(Implementation implementation) {

        if (!implementation.getTaskType().equals(TaskType.HTTP)) {
            return false;
        }

        HTTPResourceDescription hrd = (HTTPResourceDescription) implementation.getRequirements();
        return this.description.getServices().contains(hrd.getServices().get(0));
    }

    @Override
    public String getMonitoringData(String prefix) {
        return prefix + "<TotalComputingUnits></TotalComputingUnits>" + "\n";
    }

    @Override
    public Integer fitCount(Implementation impl) {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean hasAvailable(HTTPResourceDescription consumption) {
        return true;
    }

    @Override
    public boolean hasAvailableSlots() {
        return true;
    }

    @Override
    public HTTPResourceDescription reserveResource(HTTPResourceDescription consumption) {
        return consumption;
    }

    @Override
    public void releaseResource(HTTPResourceDescription consumption) {
        // does nothing
    }

    @Override
    public void releaseAllResources() {
        super.resetUsedTaskCounts();
    }

    @Override
    public Worker<HTTPResourceDescription> getSchedulingCopy() {
        return new HTTPWorker(this);
    }

    @Override
    public int compareTo(Resource t) {
        if (t == null) {
            throw new NullPointerException();
        }

        if (t.getType() == ResourceType.HTTP) {
            return 0;
        }

        return -1;
    }
}
