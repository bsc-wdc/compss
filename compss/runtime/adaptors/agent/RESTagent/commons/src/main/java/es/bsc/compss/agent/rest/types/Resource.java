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
package es.bsc.compss.agent.rest.types;

import es.bsc.compss.types.resources.MethodResourceDescription;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import javax.xml.bind.annotation.XmlRootElement;


public abstract class Resource<P, R> {

    private String name;
    private MethodResourceDescription description;

    public static final Resource createResource(String name, MethodResourceDescription description, String adaptor, Object pConf, Object rConf) {
        switch (adaptor) {
            case "es.bsc.compss.nio.master.NIOAdaptor":
                return new NIOAdaptorResource(
                        name,
                        description,
                        (es.bsc.compss.types.project.jaxb.NIOAdaptorProperties) pConf,
                        (es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties) rConf
                );
            default:
                return new ExternalAdaptorResource(
                        name,
                        description,
                        adaptor,
                        (es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties) pConf,
                        (es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties) rConf);
        }

    }

    public Resource() {
    }

    public Resource(String name, MethodResourceDescription description) {
        this.name = name;
        this.description = description;

    }

    @XmlAttribute
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MethodResourceDescription getDescription() {
        return description;
    }

    public void setDescription(MethodResourceDescription description) {
        this.description = description;
    }

    public abstract String getAdaptor();

    public abstract P getProjectConf();

    public abstract R getResourcesConf();

    @Override
    public String toString() {
        return "\nName:" + name + "\nDescription:" + description;
    }


    @XmlRootElement(name = "externalResource")
    public static class ExternalAdaptorResource extends Resource<es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties, es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties> {

        @XmlElement
        private String adaptor;
        
        private es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties projectConf;
        
        private es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties resourcesConf;

        public ExternalAdaptorResource() {
        }

        public ExternalAdaptorResource(String name, MethodResourceDescription description,
                String adaptor,
                es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties projectConf,
                es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties resourcesConf) {
            super(name, description);
            this.adaptor = adaptor;
            this.projectConf = projectConf;
            this.resourcesConf = resourcesConf;
        }

        @Override
        public String getAdaptor() {
            return adaptor;
        }

        @Override
        public es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties getProjectConf() {
            return this.projectConf;
        }

        public void setProjectConf(es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties projectConf) {
            this.projectConf = projectConf;
        }

        @Override
        public es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties getResourcesConf() {
            return this.resourcesConf;
        }

        public void setResourcesConf(es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties resourcesConf) {
            this.resourcesConf = resourcesConf;
        }
    }


    @XmlRootElement(name = "nioResource")
    public static class NIOAdaptorResource extends Resource<es.bsc.compss.types.project.jaxb.NIOAdaptorProperties, es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties> {

        private es.bsc.compss.types.project.jaxb.NIOAdaptorProperties projectConf;
        private es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties resourcesConf;

        public NIOAdaptorResource() {
        }

        public NIOAdaptorResource(String name, MethodResourceDescription description,
                es.bsc.compss.types.project.jaxb.NIOAdaptorProperties projectConf,
                es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties resourcesConf) {
            super(name, description);
            this.projectConf = projectConf;
            this.resourcesConf = resourcesConf;
        }

        @Override
        public String getAdaptor() {
            return "es.bsc.compss.nio.master.NIOAdaptor";
        }

        @Override
        public es.bsc.compss.types.project.jaxb.NIOAdaptorProperties getProjectConf() {
            return this.projectConf;
        }

        public void setProjectConf(es.bsc.compss.types.project.jaxb.NIOAdaptorProperties projectConf) {
            this.projectConf = projectConf;
        }

        @Override
        public es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties getResourcesConf() {
            return this.resourcesConf;
        }

        public void setResourcesConf(es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties resourcesConf) {
            this.resourcesConf = resourcesConf;
        }
    }
}
