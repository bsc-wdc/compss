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


public class StorageException extends Exception implements Serializable {

    private static final long serialVersionUID = 1L;


    /**
     * Creates an empty storage exception.
     */
    public StorageException() {
        super();
    }

    /**
     * Creates a storage exception with the given message and nested exception.
     * 
     * @param message Exception message.
     * @param cause Nested exception.
     */
    public StorageException(String message, Exception cause) {
        super(message, cause);
    }

    /**
     * Creates a storage exception with the given message.
     * 
     * @param message Exception message.
     */
    public StorageException(String message) {
        super(message);
    }

    /**
     * Creates a storage exception with the nested exception.
     * 
     * @param cause Nested exception.
     */
    public StorageException(Exception cause) {
        super(cause);
    }

}
