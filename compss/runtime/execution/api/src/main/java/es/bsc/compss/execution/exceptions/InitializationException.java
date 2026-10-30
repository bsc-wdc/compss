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
package es.bsc.compss.execution.exceptions;

/**
 * Error occurred during the initialization of an execution component. Thrown when a component of the execution engine
 * cannot be initialized with the provided configuration.
 */
public class InitializationException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    /**
     * Creates a new InitializationException with a message.
     *
     * @param message Description of the error.
     */
    public InitializationException(String message) {
        super(message);
    }

    /**
     * Creates a new InitializationException with a nested exception.
     *
     * @param e Nested exception.
     */
    public InitializationException(Exception e) {
        super(e);
    }

    /**
     * Creates a new InitializationException with a message and a nested exception.
     *
     * @param msg Description of the error.
     * @param e Nested exception.
     */
    public InitializationException(String msg, Exception e) {
        super(msg, e);
    }

}
