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
package es.bsc.compss.types.project.exceptions;

public class ProjectFileValidationException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    public ProjectFileValidationException(Exception e) {
        super(e);
    }

    public ProjectFileValidationException(String msg) {
        super(msg);
    }

    public ProjectFileValidationException(String property, String parent) {
        super("Invalid property " + property + " with parent element " + parent);
    }

}
