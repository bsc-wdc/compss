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
import es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlSeeAlso;


@XmlRootElement(name = "externalResource")
@XmlSeeAlso({ ExternalAdaptorProperties.class,
    ResourcesExternalAdaptorProperties.class })
public class ExternalAdaptorResource extends Resource<ExternalAdaptorProperties, ResourcesExternalAdaptorProperties> {

    public ExternalAdaptorResource() {
    }

    public ExternalAdaptorResource(String name, MethodResourceDescription description, String adaptor,
        ExternalAdaptorProperties projectConf, ResourcesExternalAdaptorProperties resourcesConf) {
        super(name, description, adaptor, projectConf, resourcesConf);
    }

}
