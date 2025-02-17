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
package es.bsc.compss.gos.master.configuration;

import es.bsc.compss.gos.master.GOSAdaptor;
import es.bsc.compss.gos.master.exceptions.GOSException;
import es.bsc.compss.gos.master.monitoring.GOSMonitoring;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;

import java.util.HashMap;
import java.util.Map;


public class GOSConfiguration extends MethodConfiguration {

    public static final int DEFAULT_SSH_PORT = 22;
    private final GOSMonitoring monitoring;
    private Map<String, Object> resourcesProperties;
    private Map<String, Object> projectProperties;
    private int port = DEFAULT_SSH_PORT;
    private boolean isBatch;
    private final GOSAdaptor adaptor;


    /**
     * Instantiates a new Gos configuration.
     *
     * @param adaptor the adaptor GOS used
     */
    public GOSConfiguration(GOSAdaptor adaptor, GOSMonitoring gosMonitoring) {
        super(GOSAdaptor.ID);
        this.adaptor = adaptor;
        this.monitoring = gosMonitoring;
    }

    /**
     * Clones to a new Gos configuration.
     *
     * @param clone the GOSConfiguration to clone.
     */
    public GOSConfiguration(GOSConfiguration clone) {
        super(clone);
        this.adaptor = clone.adaptor;
        this.monitoring = clone.monitoring;
        this.projectProperties = clone.projectProperties;
        this.resourcesProperties = clone.resourcesProperties;
        this.isBatch = clone.isBatch;
    }

    /**
     * Project properties, and determines if the submissionMode is batch.
     *
     * @param prop the project properties
     */
    public void addProjectProperties(Map<String, Object> prop) {
        if (prop == null) {
            prop = new HashMap<>();
        }
        this.projectProperties = prop;
        this.setPort();
        if (!projectProperties.containsKey("Interactive")) {
            LOGGER.warn("[GOSCONFIGURATION] Not clear if is interactive or batch, defaulting " + "to interactive");
            isBatch = false;
        } else {
            isBatch = !(boolean) this.projectProperties.get("Interactive");
        }

    }

    /**
     * Add resources properties.
     *
     * @param resourcesProperties the resources properties
     */
    public void addResourcesProperties(Map<String, Object> resourcesProperties) {
        if (resourcesProperties == null) {
            resourcesProperties = new HashMap<>();
        }
        this.resourcesProperties = resourcesProperties;
    }

    public GOSAdaptor getAdaptor() {
        return this.adaptor;
    }

    public GOSMonitoring getMonitoring() {
        return this.monitoring;
    }

    public boolean isBatch() {
        return isBatch;
    }

    /**
     * Gets a project property given a key, returns null if it doesn't exist.
     *
     * @param key the key
     * @return the project property
     */
    public Object getProjectProperty(String key) {
        if (projectProperties.containsKey(key)) {
            return projectProperties.get(key);
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.warn(key + " key not in project properties");
            }
            return null;
        }
    }

    public Map<String, Object> getProjectProperty() {
        return projectProperties;
    }

    public Map<String, Object> getResourcesProperties() {
        return resourcesProperties;
    }

    /**
     * Gets the port for ssh connection.
     *
     * @return the port
     */
    public int getPort() {
        return this.port;
    }

    /**
     * Sets port.
     */
    private void setPort() {
        Object p = projectProperties.get("Port");
        if (p != null) {
            port = (int) p;
        } else {
            port = DEFAULT_SSH_PORT;
        }
    }
}
