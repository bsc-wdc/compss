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
package es.bsc.compss.types.resources.updates;

import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.concurrent.Semaphore;


public class BusyResources<T extends WorkerResourceDescription> extends ResourceUpdate<T> {

    private final Semaphore sem;


    public BusyResources(T busy) {
        super(busy);
        this.sem = new Semaphore(0);
    }

    @Override
    public Type getType() {
        return Type.BUSY;
    }

    @Override
    public boolean checkCompleted() {
        return sem.tryAcquire();
    }

    @Override
    public void waitForCompletion() throws InterruptedException {
        sem.acquire();
    }

    public void notifyCompletion() {
        sem.release();
    }
}
