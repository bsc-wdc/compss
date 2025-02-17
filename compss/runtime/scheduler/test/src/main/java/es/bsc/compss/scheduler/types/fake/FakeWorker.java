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
package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceType;
import es.bsc.compss.types.resources.Worker;


public class FakeWorker extends Worker<FakeResourceDescription> {

    private final FakeResourceDescription available;


    public FakeWorker(String name, FakeResourceDescription description, int limitOfTasks) {
        super(name, description, new FakeNode(name, null), limitOfTasks, null);
        this.available = new FakeResourceDescription(description.getCoreCount());
    }

    @Override
    public ResourceType getType() {
        return ResourceType.WORKER;
    }

    @Override
    public int compareTo(Resource rsrc) {
        return 0;
    }

    @Override
    public String getMonitoringData(String prefix) {
        return "";
    }

    @Override
    public boolean canRun(Implementation implementation) {
        return this.description.getCoreCount() >= ((FakeResourceDescription) implementation.getRequirements())
            .getCoreCount();
    }

    @Override
    public Integer fitCount(Implementation impl) {
        return 10;
    }

    @Override
    public boolean hasAvailable(FakeResourceDescription consumption) {
        synchronized (this.available) {
            return (consumption.getCoreCount() <= this.available.getCoreCount());
        }
    }

    @Override
    public boolean hasAvailableSlots() {
        return true;
    }

    @Override
    public FakeResourceDescription reserveResource(FakeResourceDescription consumption) {
        if (this.hasAvailable(consumption)) {
            return (FakeResourceDescription) this.available.reduceDynamic(consumption);
        } else {
            return null;
        }
    }

    @Override
    public void releaseResource(FakeResourceDescription consumption) {
        synchronized (this.available) {
            this.available.increaseDynamic(consumption);
        }
    }

    @Override
    public void releaseAllResources() {
        synchronized (this.available) {
            this.available.reduceDynamic(this.available);
            this.available.increaseDynamic(this.description);
        }
    }

    @Override
    public FakeWorker getSchedulingCopy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public FakeResourceDescription getAvailable() {
        return this.available;
    }
}
