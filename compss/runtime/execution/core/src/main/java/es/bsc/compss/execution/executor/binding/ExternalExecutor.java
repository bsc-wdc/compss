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
package es.bsc.compss.execution.executor.binding;

import es.bsc.compss.execution.platform.mirrors.ExecutorContextMirror;

/**
 * Runtime-side representation of an out-of-process executor. It owns the lifecycle state of the represented executor
 * and its registration with the mirror that created it; command traffic with the represented executor goes through the
 * accessor exposed by the concrete type (a command channel for piped executors, an owning thread for persistent ones).
 * Lifecycle: an executor starts at CREATING, becomes RUNNING once deployed, enters STOPPING when its shutdown starts
 * and reaches DELETED once it is gone; terminated() can be called from any state and transitions simply overwrite the
 * current state. An executor accepts tasks (canRun()) while it is CREATING or RUNNING. The state machine is part of the
 * contract: the transition methods and canRun() are final, so every executor family honors exactly one lifecycle.
 */
public abstract class ExternalExecutor {

    /**
     * Lifecycle states of the represented executor.
     */
    private enum State {
        CREATING, RUNNING, STOPPING, DELETED
    }


    private volatile State state;


    /**
     * Constructs a new executor handle at state CREATING.
     */
    protected ExternalExecutor() {
        this.state = State.CREATING;
    }

    /**
     * Registers that the out-of-process executor is fully deployed.
     */
    public final void deployed() {
        this.state = State.RUNNING;
    }

    /**
     * Registers that the out-of-process executor is being shut down. From this moment the executor no longer accepts
     * tasks.
     */
    public final void stopping() {
        this.state = State.STOPPING;
    }

    /**
     * Registers that the out-of-process executor is gone.
     */
    public final void terminated() {
        this.state = State.DELETED;
    }

    /**
     * Returns whether the represented executor can run tasks or not. An executor accepts tasks while it is being
     * created or fully deployed.
     *
     * @return {@literal true} if the executor accepts tasks; {@literal false} otherwise
     */
    public final boolean canRun() {
        return this.state == State.CREATING || this.state == State.RUNNING;
    }

    /**
     * Returns the mirror that registered this executor.
     *
     * @return mirror to whom the executor belongs
     */
    public abstract ExecutorContextMirror<?> getMirror();

}
