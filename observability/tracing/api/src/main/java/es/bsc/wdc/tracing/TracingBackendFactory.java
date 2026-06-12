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

package es.bsc.wdc.tracing;

public interface TracingBackendFactory {

    String WARN_IT_FILE_NOT_READ = "WARNING: COMPSs Properties file could not be read";


    /**
     * Returns the type of supported tracing system.
     *
     * @return type of supported tracing system
     */
    String getType();

    /**
     * Creates the TracingBackend based on the properties defined in a file.
     *
     * @param installDir location where the tracing system is installed
     * @param hostId identifier of the host
     * @param nodeName label of the node in the tracing system
     * @param configLocation Path containing a properties file
     * @return the instance of the backend
     */
    TracingBackend create(String installDir, int hostId, String nodeName, String configLocation);

}
