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

import es.bsc.compss.types.resources.ResourceDescription;


public abstract class ResourceUpdate<T extends ResourceDescription> {

    public static enum Type {
        INCREASE, // Increasing resource capabilities
        REDUCE, // Reducing resource capabilities
        IDLE, // Idle resources in the worker (due to a compss_wait_on for example)
        BUSY // Reclaim previously idle resources
    }


    private final T modification;


    protected ResourceUpdate(T modification) {
        this.modification = modification;
    }

    public final T getModification() {
        return modification;
    }

    public abstract Type getType();

    public abstract boolean checkCompleted();

    public abstract void waitForCompletion() throws InterruptedException;

}
