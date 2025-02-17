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
package es.bsc.compss.agent.rest.types;

import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.types.resources.MethodResourceDescription;


/**
 * Class describing the resources available on a node and the necessary configuration (Adaptor name, project's and
 * resource's configuration values.
 */
public class RESTResource {

    /**
     * Constructs a new RESTResource instance for the resources on the node with name {@code name}.
     *
     * @param name Name of the node.
     * @param description Description of the resources of the node.
     * @param adaptor Adaptor to interact with such node.
     * @param pConf Configuration values related to the project file.
     * @param rConf Configuration values related to the resources file.
     * @return RESTResource Instance constructed with the values passed in as parameter.
     */
    public static final Resource<?, ?> createResource(String name, MethodResourceDescription description,
        String adaptor, Object pConf, Object rConf) {

        switch (adaptor) {
            case "es.bsc.compss.nio.master.NIOAdaptor":
                return new NIOAdaptorResource(name, description,
                    (es.bsc.compss.types.project.jaxb.NIOAdaptorProperties) pConf,
                    (es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties) rConf);
            default:
                return new ExternalAdaptorResource(name, description, adaptor,
                    (es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties) pConf,
                    (es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties) rConf);
        }

    }

    private RESTResource() {
        // Private constructor to avoid instantiation.
    }

}
