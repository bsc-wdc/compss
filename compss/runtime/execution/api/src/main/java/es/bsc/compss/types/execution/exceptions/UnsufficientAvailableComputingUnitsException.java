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
 * Error occurred when a computing units request cannot be satisfied with the computing units available on the node.
 * Thrown by the {@code ThreadBinder} contract when a job requests more computing units than the binder can assign.
 */
public class UnsufficientAvailableComputingUnitsException extends UnsufficientAvailableResourcesException {

    /**
     * Exception Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    /**
     * Creates a new UnsufficientAvailableComputingUnitsException with a message.
     *
     * @param message Description of the error.
     */
    public UnsufficientAvailableComputingUnitsException(String message) {
        super(message);
    }

    /**
     * Creates a new UnsufficientAvailableComputingUnitsException with a nested exception.
     *
     * @param e Nested exception.
     */
    public UnsufficientAvailableComputingUnitsException(Exception e) {
        super(e);
    }

    /**
     * Creates a new UnsufficientAvailableComputingUnitsException with a message and a nested exception.
     *
     * @param msg Description of the error.
     * @param e Nested exception.
     */
    public UnsufficientAvailableComputingUnitsException(String msg, Exception e) {
        super(msg, e);
    }

}
