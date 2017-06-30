package integratedtoolkit.connectors;

import es.bsc.conn.Connector;
import es.bsc.conn.types.VirtualResource;
import integratedtoolkit.ITConstants;
import integratedtoolkit.connectors.conn.util.ConnectorProxy;
import integratedtoolkit.connectors.conn.util.Converter;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.CloudProvider;
import integratedtoolkit.types.resources.description.CloudImageDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.util.Classpath;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Default SSH Connector implementation to use specific SSH connectors' interface
 *
 */
public class DefaultNoSSHConnector extends AbstractConnector {

    private static final String CONNECTORS_REL_PATH = File.separator + "Runtime" + File.separator + "cloud-conn" + File.separator;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.CONNECTORS);
    private static final String WARN_NO_IT_HOME = "WARN: IT_HOME not defined, no default connectors loaded";

    // Constraints default values
    private static final float UNASSIGNED_FLOAT = -1.0f;

    private ConnectorProxy connector;


    /**
     * Constructs a new Default SSH Connector and instantiates the specific connector implementation
     *
     * @param provider
     * @param connectorJarPath
     * @param connectorMainClass
     * @param connectorProperties
     * @throws ConnectorException
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
                String itHome = System.getenv(ITConstants.IT_HOME);
                if (itHome == null || itHome.isEmpty()) {
                    LOGGER.warn(WARN_NO_IT_HOME);
                    return;
                }
                jarPath = itHome + CONNECTORS_REL_PATH + connectorJarPath;
            }

            // Load jar to classpath
            LOGGER.debug(" - Loading from : " + jarPath);
            Classpath.loadPath(jarPath, LOGGER);

            // Invoke connector main class
            LOGGER.debug(" - Using connector " + connectorMainClass);
            Class<?> conClass = Class.forName(connectorMainClass);
            Constructor<?> constructor = conClass.getDeclaredConstructors()[0];
            conn = (Connector) constructor.newInstance(connectorProperties);
        } catch (FileNotFoundException fnfe) {
            throw new ConnectorException("Specific Connector jar file (" + connectorJarPath + ") not found", fnfe);
        } catch (Exception e) {
            throw new ConnectorException(e);
        } finally {
            connector = new ConnectorProxy(conn);
        }
    }

    @Override
    public void destroy(Object id) throws ConnectorException {
        LOGGER.debug("Destroy connection with id " + id);
        connector.destroy(id);
    }

    @Override
    public Object create(String name, CloudMethodResourceDescription cmrd) throws ConnectorException {
        LOGGER.debug("Create connection " + name);
        return connector.create(Converter.getHardwareDescription(cmrd), Converter.getSoftwareDescription(cmrd),
                cmrd.getImage().getProperties());
    }

    @Override
    public CloudMethodResourceDescription waitUntilCreation(Object id, CloudMethodResourceDescription requested) throws ConnectorException {
        LOGGER.debug("Waiting for " + (String) id);
        VirtualResource vr = connector.waitUntilCreation(id);
        CloudMethodResourceDescription cmrd = Converter.toCloudMethodResourceDescription(vr, requested);
        LOGGER.debug("Return cloud method resource description " + cmrd.toString());
        return cmrd;
    }

    @Override
    public float getMachineCostPerTimeSlot(CloudMethodResourceDescription cmrd) {
        return connector.getPriceSlot(Converter.getVirtualResource("-1", cmrd), UNASSIGNED_FLOAT);
    }

    @Override
    public long getTimeSlot() {
        return connector.getTimeSlot(TWO_MIN);
    }

    @Override
    protected void close() {
        LOGGER.debug("Close connector");
        connector.close();
    }

    @Override
    public void configureAccess(String IP, String user, String password) throws ConnectorException {
        // TODO Nothing to do
    }

    @Override
    public void prepareMachine(String IP, CloudImageDescription cid) throws ConnectorException {
        // TODO Nothing to do
    }

}
