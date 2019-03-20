/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.Worker;


public class FakeWorker extends Worker<FakeResourceDescription, FakeImplementation> {

    private final FakeResourceDescription available;


    public FakeWorker(String name, FakeResourceDescription description, int limitOfTasks) {
        super(name, description, new FakeNode(name), limitOfTasks, null);
        available = (FakeResourceDescription) description.copy();
    }

    public FakeWorker(FakeWorker fw) {
        super(fw);
        available = (FakeResourceDescription) fw.available.copy();
    }

    @Override
    public Resource.Type getType() {
        return Resource.Type.WORKER;
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
    public boolean canRun(FakeImplementation implementation) {
        return true;
    }

    @Override
    public FakeResourceDescription reserveResource(FakeResourceDescription consumption) {
        synchronized (available) {
            if (this.hasAvailable(consumption)) {
                return (FakeResourceDescription) available.reduceDynamic(consumption);
            } else {
                return null;
            }
        }
    }

    @Override
    public void releaseResource(FakeResourceDescription consumption) {
        synchronized (available) {
            available.increaseDynamic(consumption);
        }
    }

    @Override
    public void releaseAllResources() {
        synchronized (available) {
            super.resetUsedTaskCounts();
            available.reduceDynamic(available);
            available.increaseDynamic(description);
        }
    }

    @Override
    public boolean hasAvailable(FakeResourceDescription consumption) {
        synchronized (available) {
            return available.canHost(consumption);
        }
    }

    @Override
    public boolean usesGPU(FakeResourceDescription consumption) {
        LOGGER.debug("fake worker");
        return false;
    }

    @Override
    public boolean usesFPGA(FakeResourceDescription consumption) {
        return false;
    }

    @Override
    public boolean usesOthers(FakeResourceDescription consumption) {
        return false;
    }

    @Override
    public Integer fitCount(FakeImplementation impl) {
        return 10;
    }

    @Override
    public Worker<FakeResourceDescription, FakeImplementation> getSchedulingCopy() {
        return new FakeWorker(this);
    }
}
