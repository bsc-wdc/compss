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
package es.bsc.compss.types.execution.exceptions;

/**
 * Error occurred during the execution of a job (task execution). Thrown by the execution engine and the invokers when a
 * task cannot be prepared, executed or post-processed.
 */
public class JobExecutionException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    /**
     * Creates a new JobExecutionException with a message.
     *
     * @param message Description of the error.
     */
    public JobExecutionException(String message) {
        super(message);
    }

    /**
     * Creates a new JobExecutionException with a nested exception.
     *
     * @param e Nested exception.
     */
    public JobExecutionException(Throwable e) {
        super(e);
    }

    /**
     * Creates a new JobExecutionException with a message and a nested exception.
     *
     * @param message Description of the error.
     * @param e Nested exception.
     */
    public JobExecutionException(String message, Throwable e) {
        super(message, e);
    }
}
