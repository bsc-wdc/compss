/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
 * Exception to handle errors on node start
 * 
 */
public class InitNodeException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty Init Node Exception
     * 
     */
    public InitNodeException() {
        super();
    }

    /**
     * New nested @e Init Node Exception
     * 
     * @param e
     */
    public InitNodeException(Exception e) {
        super(e);
    }

    /**
     * New Init Node Exception with message @msg
     * 
     * @param msg
     */
    public InitNodeException(String msg) {
        super(msg);
    }

}
