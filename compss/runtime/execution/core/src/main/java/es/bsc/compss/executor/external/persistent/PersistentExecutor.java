/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.executor.external.persistent;

import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.external.ExternalExecutor;

/**
 * Handle to the persistent binding of an executor thread. The represented executor is not an out-of-process executor
 * but the persistent language binding attached to the owning executor thread; its lifecycle is driven by the
 * PersistentMirror that registered it.
 */
public class PersistentExecutor extends ExternalExecutor {

    private final PersistentMirror mirror;
    private final Executor owner;


    /**
     * Constructs a new PersistentExecutor.
     * 
     * @param mirror mirror to whom it belongs
     * @param owner executor thread that owns the persistent binding
     */
    public PersistentExecutor(PersistentMirror mirror, Executor owner) {
        this.mirror = mirror;
        this.owner = owner;
    }

    @Override
    public PersistentMirror getMirror() {
        return this.mirror;
    }

    /**
     * Returns the executor thread that owns the persistent binding represented by this handle.
     * 
     * @return the owning executor thread
     */
    public Executor getOwningExecutor() {
        return this.owner;
    }

}
