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
package es.bsc.compss.types.resources;

import es.bsc.compss.log.Loggers;
import java.util.LinkedList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ResourcesPool {
    // Log and debug

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final boolean DEBUG = LOGGER.isDebugEnabled();

    // List of all created resources
    private static final List<Resource> AVAILABLE_RESOURCES = new LinkedList<>();


    /**
     * Returns the Resource associated to the given name @name. Null if any resource has been registered with the name
     *
     * @param name Resource name
     * @return
     */
    public static Resource getResource(String name) {
        for (Resource r : AVAILABLE_RESOURCES) {
            if (r.getName().equals(name)) {
                return r;
            }
        }
        LOGGER.warn("Resource with name " + name + " not found");
        return null;
    }

    public static void add(Resource res) {
        AVAILABLE_RESOURCES.add(res);
    }

}
