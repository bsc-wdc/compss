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
package es.bsc.compss.types.execution;

import es.bsc.compss.executor.Executor;


public abstract class ExecutorRequest {

    /**
     * Handles the request's execution on the given executor.
     *
     * @param executor executor running the test
     * @throws StopExecutorException the executor should stop processing request
     */
    public abstract void run(Executor executor) throws StopExecutorException;

    /**
     * Returns whether the request should be taken by the executor waiting for longer or shorter.
     *
     * @return {@literal true} if the request should be treated by the execution waiting for longer; {@literal false}
     *         otherwise.
     */
    public boolean hasAgePriority() {
        return true;
    }


    public static class StopExecutorException extends Exception {
    }


    @Override
    public abstract String toString();

}
