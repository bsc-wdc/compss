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

public class ExternalPropertyException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    /**
     * New ExternalPropertyException exception with message {@code message}.
     * 
     * @param message Exception message.
     */
    public ExternalPropertyException(String message) {
        super(message);
    }

    /**
     * New ExternalPropertyException exception with message {@code message} and nested exception {@code e}.
     * 
     * @param message Exception message.
     * @param e Nested exception.
     */
    public ExternalPropertyException(String message, Exception e) {
        super(message, e);
    }

    /**
     * New ExternalPropertyException exception with nested exception {@code e}.
     * 
     * @param e Nested exception.
     */
    public ExternalPropertyException(Exception e) {
        super(e);
    }
}
