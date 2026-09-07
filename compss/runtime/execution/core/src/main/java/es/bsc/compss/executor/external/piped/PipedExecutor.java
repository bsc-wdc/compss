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
package es.bsc.compss.executor.external.piped;

public class PipedExecutor {

    private enum State {
        CREATING, RUNNING, STOPPING, DELETED
    }


    private volatile State state;
    private final PipedMirror mirror;
    private final PipePair pipePair;


    /**
     * Constructs a new PipedExecutor.
     * 
     * @param mirror mirror to whom it belongs
     * @param pp PipePair to communicate with the PipedExecutor
     */
    public PipedExecutor(PipedMirror mirror, PipePair pp) {
        this.state = State.CREATING;
        this.pipePair = pp;
        this.mirror = mirror;
    }

    public PipedMirror getMirror() {
        return mirror;
    }

    public PipePair getPipePair() {
        return pipePair;
    }

    /**
     * Registers that the PipedExecutor is fully deployed.
     */
    public void deployed() {
        this.state = State.RUNNING;
    }

    public void stopping() {
    }

    public void terminated() {
        this.state = State.DELETED;
    }

    /**
     * Returns whether the PipedExecutor can run tasks or not.
     * 
     * @return {@literal true}, if the pipedExecutor is valid; {@literal false} otherwise.
     */
    public boolean canRun() {
        return state == State.CREATING || state == State.RUNNING;
    }

}
