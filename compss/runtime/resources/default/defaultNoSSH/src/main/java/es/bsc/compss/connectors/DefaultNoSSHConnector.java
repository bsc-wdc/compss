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
package es.bsc.compss.connectors;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.connectors.conn.util.ConnectorProxy;
import es.bsc.compss.connectors.conn.util.Converter;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.util.Classpath;

import es.bsc.conn.Connector;
import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.VirtualResource;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Default SSH Connector implementation to use specific SSH connectors' interface.
 */
public class DefaultNoSSHConnector extends AbstractConnector {

    private static final String CONNECTORS_REL_PATH =
        File.separator + "Runtime" + File.separator + "cloud-conn" + File.separator;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.CONNECTORS);
    private static final String WARN_NO_COMPSS_HOME = "WARN: COMPSS_HOME not defined, no default connectors loaded";

    // Constraints default values
    private static final float UNASSIGNED_FLOAT = -1.0f;

    private ConnectorProxy connectorProxy;


    /**
     * Constructs a new Default SSH Connector and instantiates the specific connector implementation.
     *
     * @param provider Cloud provider.
     * @param connectorJarPath Path to the connector JAR.
     * @param connectorMainClass Main class of the connector implementation.
     * @param connectorProperties Specific connector properties for initialization.
     * @throws ConnectorException When the connector instantiation fails.
     */
    public DefaultNoSSHConnector(CloudProvider provider, String connectorJarPath, String connectorMainClass,
        Map<String, String> connectorProperties) throws ConnectorException {

        super(provider, connectorProperties);

        LOGGER.info("Creating DefaultNoSSHConnector");
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("  Detected " + connectorProperties.size() + " Connector properties");
            for (Entry<String, String> prop : connectorProperties.entrySet()) {
                LOGGER.debug("   > ConnectorProperty: " + prop.getKey() + " - " + prop.getValue());
            }
        }

        Connector conn = null;

        LOGGER.debug(" - Loading " + connectorJarPath);
        try {
            // Check if its relative to CONNECTORS or absolute to system
            String jarPath = connectorJarPath;
            if (!connectorJarPath.startsWith(File.separator)) {
                String compssHome = System.getenv(COMPSsConstants.COMPSS_HOME);
                if (compssHome == null || compssHome.isEmpty()) {
                    LOGGER.warn(WARN_NO_COMPSS_HOME);
                    return;
                }
                jarPath = compssHome + CONNECTORS_REL_PATH + connectorJarPath;
            }

            // Load jar to classpath
            LOGGER.debug(" - Loading from : " + jarPath);
            Classpath.loadJarsInPath(jarPath, LOGGER);

            // Invoke connector main class
            LOGGER.debug(" - Using connector " + connectorMainClass);
            Class<?> conClass = Class.forName(connectorMainClass);
            Constructor<?> constructor = conClass.getDeclaredConstructors()[0];
            conn = (Connector) constructor.newInstance(connectorProperties);
            LOGGER.debug("Ending connector handling");
        } catch (Exception e) {
            LOGGER.error("Exception creating connector", e);
            throw new ConnectorException(e);
        } finally {
            if (conn == null) {
                LOGGER.fatal("Connector constructor null");
            }
            this.connectorProxy = new ConnectorProxy(conn);
        }
    }

    @Override
    public boolean isAutomaticScalingEnabled() {
        return this.connectorProxy.isAutomaticScalingEnabled();
    }

    @Override
    public void destroy(Object id) throws ConnectorException {
        LOGGER.debug("Destroy connectorProxy with id " + id);
        this.connectorProxy.destroy(id);
    }

    @Override
    public Object create(String name, CloudMethodResourceDescription cmrd) throws ConnectorException {
        LOGGER.debug("Create connectorProxy " + name);

        HardwareDescription hd = Converter.getHardwareDescription(cmrd);
        SoftwareDescription sd = Converter.getSoftwareDescription(cmrd);
        return this.connectorProxy.create(name, hd, sd, cmrd.getImage().getProperties());
    }

    @Override
    public CloudMethodResourceDescription waitUntilCreation(Object id, CloudMethodResourceDescription requested)
        throws ConnectorException {

        LOGGER.debug("Waiting connectorProxy with id " + id);

        VirtualResource vr = this.connectorProxy.waitUntilCreation(id);
        CloudMethodResourceDescription cmrd = Converter.toCloudMethodResourceDescription(vr, requested);
        LOGGER.debug("Return cloud method resource description " + cmrd.toString());
        return cmrd;
    }

    @Override
    public float getMachineCostPerTimeSlot(CloudMethodResourceDescription cmrd) {
        return this.connectorProxy.getPriceSlot(Converter.getVirtualResource("-1", cmrd), UNASSIGNED_FLOAT);
    }

    @Override
    public long getTimeSlot() {
        return this.connectorProxy.getTimeSlot(TWO_MIN);
    }

    @Override
    protected void close() {
        LOGGER.debug("Close connector");

        this.connectorProxy.close();
    }

    @Override
    public void configureAccess(String ip, String user, String password) throws ConnectorException {
        // Nothing to do
    }

    @Override
    public void prepareMachine(String ip, CloudImageDescription cid) throws ConnectorException {
        // Nothing to do
    }

}
