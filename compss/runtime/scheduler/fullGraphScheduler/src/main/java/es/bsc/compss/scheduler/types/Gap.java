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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.resources.ResourceDescription;


public class Gap {

    private final long initialTime;
    private long endTime;
    private final AllocatableAction origin;
    private final ResourceDescription resources;
    private final int capacity;


    public Gap(long start, AllocatableAction origin, ResourceDescription resources, int capacity) {
        this.initialTime = start;
        this.origin = origin;
        this.resources = resources.copy();
        this.capacity = capacity;
    }

    public Gap(long start, long endTime, AllocatableAction origin, ResourceDescription resources, int capacity) {
        this.initialTime = start;
        this.endTime = endTime;
        this.origin = origin;
        this.resources = resources.copy();
        this.capacity = capacity;
    }

    public long getInitialTime() {
        return this.initialTime;
    }

    public long getEndTime() {
        return this.endTime;
    }

    public AllocatableAction getOrigin() {
        return this.origin;
    }

    public ResourceDescription getResources() {
        return this.resources;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int getCapacity() {
        return this.capacity;
    }

    @Override
    public String toString() {
        return "<" + this.initialTime + "->" + this.endTime + ", " + this.origin + ", " + this.resources + ", with "
                + this.capacity + " slots >";
    }

}
