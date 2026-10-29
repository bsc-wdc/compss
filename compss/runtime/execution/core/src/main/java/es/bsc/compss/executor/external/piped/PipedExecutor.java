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

import es.bsc.compss.executor.external.ExternalExecutor;

/**
 * Handle to an out-of-process executor connected through a piped command channel. All command traffic with the
 * represented executor goes through the pipe pair passed at construction.
 */
public class PipedExecutor extends ExternalExecutor {

    private final PipedMirror mirror;
    private final PipePair pipePair;


    /**
     * Constructs a new PipedExecutor.
     * 
     * @param mirror mirror to whom it belongs
     * @param pp PipePair to communicate with the PipedExecutor
     */
    public PipedExecutor(PipedMirror mirror, PipePair pp) {
        this.mirror = mirror;
        this.pipePair = pp;
    }

    @Override
    public PipedMirror getMirror() {
        return this.mirror;
    }

    /**
     * Returns the pipe pair to communicate with the represented out-of-process executor.
     * 
     * @return command channel of the represented executor
     */
    public PipePair getPipePair() {
        return this.pipePair;
    }

}
