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
 * Exception to creation/deletion announce errors.
 */
public class AnnounceException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty Announce Exception.
     */
    public AnnounceException() {
        super();
    }

    /**
     * New nested {@code e} Announce Exception.
     * 
     * @param e Exception.
     */
    public AnnounceException(Exception e) {
        super(e);
    }

    /**
     * New Announce Exception with message {@code msg}.
     * 
     * @param msg Exception message.
     */
    public AnnounceException(String msg) {
        super(msg);
    }

}
