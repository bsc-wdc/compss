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
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class FakeResourceDescription extends WorkerResourceDescription {

    private int coreCount = 0;


    public FakeResourceDescription(int coreCount) {
        this.coreCount = coreCount;
    }

    @Override
    public boolean canHost(Implementation impl) {
        FakeResourceDescription desc = (FakeResourceDescription) impl.getRequirements();
        return canHost(desc);
    }

    public boolean canHost(FakeResourceDescription desc) {
        return !(desc.coreCount > this.coreCount);
    }

    public boolean canHostDynamic(Implementation impl) {
        FakeResourceDescription desc = (FakeResourceDescription) impl.getRequirements();
        return !(desc.coreCount > this.coreCount);
    }

    @Override
    public void increase(ResourceDescription rd) {
        FakeResourceDescription desc = (FakeResourceDescription) rd;
        this.coreCount += desc.coreCount;
    }

    @Override
    public void reduce(ResourceDescription rd) {
        FakeResourceDescription desc = (FakeResourceDescription) rd;
        this.coreCount -= desc.coreCount;
    }

    @Override
    public void increaseDynamic(ResourceDescription rd) {
        FakeResourceDescription desc = (FakeResourceDescription) rd;
        // int oldCount = this.coreCount;
        this.coreCount += desc.coreCount;
    }

    @Override
    public ResourceDescription reduceDynamic(ResourceDescription rd) {
        FakeResourceDescription desc = (FakeResourceDescription) rd;
        // int oldCount = this.coreCount;
        this.coreCount -= desc.coreCount;
        return desc;
    }

    @Override
    public boolean isDynamicUseless() {
        return coreCount == 0;
    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription other) {
        FakeResourceDescription otherFake = (FakeResourceDescription) other;
        int coreCommons = Math.min(coreCount, otherFake.coreCount);
        return new FakeResourceDescription(coreCommons);
    }

    @Override
    public ResourceDescription copy() {
        return new FakeResourceDescription(coreCount);
    }

    @Override
    public String toString() {
        return this.coreCount + " cores";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.coreCount = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(coreCount);
    }

    public boolean checkEquals(FakeResourceDescription fakeResourceDescription) {
        return this.coreCount == fakeResourceDescription.coreCount;
    }

    @Override
    public void mimic(ResourceDescription rd) {
        FakeResourceDescription frd = (FakeResourceDescription) rd;
        this.coreCount = frd.coreCount;
    }

    @Override
    public boolean isDynamicConsuming() {
        return false;
    }

    @Override
    public String getDynamicDescription() {
        return null;
    }

    @Override
    public boolean usesCPUs() {
        return true;
    }

    @Override
    public void scaleUpBy(int n) {

    }

    @Override
    public void scaleDownBy(int n) {

    }

    public int getCoreCount() {
        return coreCount;
    }

}
