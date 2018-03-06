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
package es.bsc.compss.api;

/**
 * COMPSs API Class for JAVA
 *
 */
public class COMPSs {

    private static final String SKIP_MESSAGE = "COMPSs Runtime is not loaded. Skipping call";


    /**
     * Barrier
     * 
     */
    public static void barrier() {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

}
