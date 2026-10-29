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
package es.bsc.compss.types.execution;

import es.bsc.compss.types.data.location.ProtocolType;

/**
 * Description of a physical location of the source data of a parameter: the access protocol, the path of the data and
 * the host holding it.
 */
public interface InvocationParamURI {

    /**
     * Returns whether the URI is located in the given host.
     *
     * @param hostName Host name to check.
     * @return {@literal true} if the data is located in the host, {@literal false} otherwise.
     */
    boolean isHost(String hostName);

    /**
     * Returns the access protocol of the URI.
     *
     * @return The access protocol.
     */
    ProtocolType getProtocol();

    /**
     * Returns the path of the data.
     *
     * @return The data path.
     */
    String getPath();

}
