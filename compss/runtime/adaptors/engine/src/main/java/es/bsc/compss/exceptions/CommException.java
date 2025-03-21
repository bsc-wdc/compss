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
package es.bsc.compss.exceptions;

/**
 * Class representing an exception raised due to some problem within the Comm layer.
 */
public class CommException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    /**
     * Creates a new CommException.
     */
    public CommException() {
        super();
    }

    /**
     * Creates a new CommException with a nested exception.
     * 
     * @param e Nested exception.
     */
    public CommException(Exception e) {
        super(e);
    }

    /**
     * Creates a new CommException with a custom message.
     */
    public CommException(String msg) {
        super(msg);
    }

    /**
     * Creates a new CommException with a custom message and a nested exception.
     * 
     * @param msg Custom message.
     * @param e Nested exception.
     */
    public CommException(String msg, Exception e) {
        super(msg, e);
    }
}
