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
package storage;

import java.io.Serializable;


/**
 * Exception representation for errors when calling the Storage ITF. TODO: complete javadoc
 */
public class StorageException extends Exception implements Serializable {

    private static final long serialVersionUID = 1L;


    /**
     * New empty storage exception.
     */
    public StorageException() {

    }

    /**
     * New storage exception with message @message.
     * 
     * @param message String
     */
    public StorageException(String message) {
        super(message);
    }

    /**
     * New storage exception for nested exception @cause.
     * 
     * @param cause Exception
     */
    public StorageException(Exception cause) {
        super(cause);
    }

    /**
     * New storage exception with message @message and nested exception @cause.
     * 
     * @param message String
     * @param cause Exception
     */
    public StorageException(String message, Exception cause) {
        super(message, cause);
    }

}
