package integratedtoolkit.connectors;

import es.bsc.conn.Connector;
import es.bsc.conn.exceptions.ConnException;
import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.Processor;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.VirtualResource;
import integratedtoolkit.ITConstants;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.util.Classpath;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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
    private static final String ERROR_NO_CONN = "ERROR: Connector specific implementation is null";

    // Constraints default values
    private static final String CPU_TYPE = "CPU";
    private static final float UNASSIGNED_FLOAT = -1.0f;

    private final Connector connector;


    /**
     * Constructs a new Default SSH Connector and instantiates the specific connector implementation
     * 
     * @param providerName
     * @param connectorJarPath
     * @param connectorMainClass
     * @param connectorProperties
     * @throws ConnectorException
     */
    public DefaultNoSSHConnector(String providerName, String connectorJarPath, String connectorMainClass, 
            HashMap<String, String> connectorProperties) throws ConnectorException {
        
        super(providerName, connectorProperties);
        
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
            connector = conn;
        }
        
        if (connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
    }

    private Processor getProcessor(integratedtoolkit.types.resources.components.Processor p) {
        return new Processor(p.getName(), p.getComputingUnits(), p.getSpeed(), p.getArchitecture(), p.getPropName(), p.getPropValue());
    }

    private integratedtoolkit.types.resources.components.Processor getProcessor(Processor p) {
        // FIXME Assuming that all processors have type CPU and no mem
        return new integratedtoolkit.types.resources.components.Processor(p.getName(), p.getComputingUnits(), p.getSpeed(),
                p.getArchitecture(), CPU_TYPE, UNASSIGNED_FLOAT, p.getPropName(), p.getPropValue());
    }

    private List<Processor> getConnectorProcessors(List<integratedtoolkit.types.resources.components.Processor> processorList) {
        List<Processor> processors = new LinkedList<Processor>();
        for (integratedtoolkit.types.resources.components.Processor p : processorList) {
            processors.add(getProcessor(p));
        }
        return processors;
    }

    private List<integratedtoolkit.types.resources.components.Processor> getCompssProcessors(List<Processor> processorList) {
        List<integratedtoolkit.types.resources.components.Processor> processors = new LinkedList<integratedtoolkit.types.resources.components.Processor>();

        for (Processor p : processorList) {
            processors.add(getProcessor(p));
        }
        return processors;
    }

    private HardwareDescription getHardwareDescription(CloudMethodResourceDescription cmrd) {
        // FIXME Using CPU Computing units, should check all units
        return new HardwareDescription(getConnectorProcessors(cmrd.getProcessors()), cmrd.getTotalCPUComputingUnits(), cmrd.getMemorySize(),
                cmrd.getMemoryType(), cmrd.getStorageSize(), cmrd.getStorageType(), cmrd.getPriceTimeUnit(), cmrd.getPricePerUnit(),
                cmrd.getImage().getImageName(), cmrd.getType(), cmrd.getImage().getProperties());
    }

    private SoftwareDescription getSoftwareDescription(CloudMethodResourceDescription cmrd) {
        return new SoftwareDescription(cmrd.getOperatingSystemType(), cmrd.getOperatingSystemDistribution(),
                cmrd.getOperatingSystemVersion(), cmrd.getAppSoftware());
    }

    private VirtualResource getVirtualResource(Object id, CloudMethodResourceDescription cmrd) {
        return new VirtualResource((String) id, getHardwareDescription(cmrd), getSoftwareDescription(cmrd),
                cmrd.getImage().getProperties());
    }

    private CloudImageDescription getCloudImageDescription(HardwareDescription hd, SoftwareDescription sd, String provider) {
        LOGGER.debug("Generating Image - Provider: "+ provider + " image: "+ hd.getImageName());
    	CloudImageDescription cid = new CloudImageDescription(provider, hd.getImageName(), hd.getImageProperties());
        cid.setOperatingSystemType(sd.getOperatingSystemType());
        cid.setOperatingSystemDistribution(sd.getOperatingSystemDistribution());
        cid.setOperatingSystemVersion(sd.getOperatingSystemVersion());
        cid.setAppSoftware(sd.getAppSoftware());
        cid.setPricePerUnit(hd.getPricePerUnit());
        cid.setPriceTimeUnit(hd.getPriceTimeUnit());
        return cid;
    }

    private void setCloudImageDescription(CloudImageDescription to, CloudImageDescription from) {
        to.setConfig(from.getConfig());
        to.setQueues(from.getQueues());
        to.setSharedDisks(from.getSharedDisks());
        to.setPackages(from.getPackages());
    }

    private void setHardwareInResourceDescription(CloudMethodResourceDescription cmrd, HardwareDescription hd) {
        cmrd.setProcessors(getCompssProcessors(hd.getProcessors()));
        cmrd.setMemorySize(hd.getMemorySize());
        cmrd.setMemoryType(hd.getMemoryType());
        cmrd.setStorageSize(hd.getStorageSize());
        cmrd.setStorageType(hd.getStorageType());
        cmrd.setPricePerUnit(hd.getPricePerUnit());
        cmrd.setPriceTimeUnit(hd.getPriceTimeUnit());
        cmrd.setType(hd.getImageType());
    }

    private void setSoftwareInResourceDescription(CloudMethodResourceDescription cmrd, SoftwareDescription sd) {
        cmrd.setOperatingSystemType(sd.getOperatingSystemType());
        cmrd.setOperatingSystemDistribution(sd.getOperatingSystemDistribution());
        cmrd.setOperatingSystemVersion(sd.getOperatingSystemVersion());
        cmrd.setAppSoftware(sd.getAppSoftware());
    }

    private CloudMethodResourceDescription toCloudMethodResourceDescription(VirtualResource vr, CloudMethodResourceDescription requested) {
        String provider = requested.getProviderName();
        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(provider, vr.getIp(), vr.getHd().getImageName());
        setHardwareInResourceDescription(cmrd, vr.getHd());
        setSoftwareInResourceDescription(cmrd, vr.getSd());
        cmrd.setImage(getCloudImageDescription(vr.getHd(), vr.getSd(), provider));
        setCloudImageDescription(cmrd.getImage(), requested.getImage());
        return cmrd;
    }

    @Override
    public void destroy(Object id) throws ConnectorException {
        LOGGER.debug("Destroy connection with id " + id);
        if (connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
        
        connector.destroy(id);
        LOGGER.debug("Connection with id " + id +  " destroyed.");
    }

    @Override
    public Object create(String name, CloudMethodResourceDescription cmrd) throws ConnectorException {
        LOGGER.debug("Create VM " + name + " with image: " + cmrd.getImage().getImageName());
        if (connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
        
        Object created;
        try {
            created = connector.create(getHardwareDescription(cmrd), getSoftwareDescription(cmrd), cmrd.getImage().getProperties());
        } catch (ConnException ce) {
            throw new ConnectorException(ce);
        }
        return created;
    }

    @Override
    public CloudMethodResourceDescription waitUntilCreation(Object id, CloudMethodResourceDescription requested) throws ConnectorException {
        LOGGER.debug("Waiting for " + (String) id);
        if (connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
        
        VirtualResource vr;
        try {
            vr = connector.waitUntilCreation(id);
        } catch (ConnException ce) {
            throw new ConnectorException(ce);
        }
        CloudMethodResourceDescription cmrd = toCloudMethodResourceDescription(vr, requested);
        LOGGER.debug("Return cloud method resource description " + cmrd.toString());
        return cmrd;
    }

    @Override
    public float getMachineCostPerTimeSlot(CloudMethodResourceDescription cmrd) {
        if (connector == null) {
            return UNASSIGNED_FLOAT;
        }
        
        return connector.getPriceSlot(getVirtualResource("-1", cmrd));
    }

    @Override
    public long getTimeSlot() {
        if (connector == null) {
            return HALF_MIN;
        }
        
        return connector.getTimeSlot();
    }

    @Override
    protected void close() {
        LOGGER.debug("Close connector");
        connector.close();
    }

	@Override
	public void configureAccess(String IP, String user, String password)
			throws ConnectorException {
		// TODO Nothing to do
		
	}

	@Override
	public void prepareMachine(String IP, CloudImageDescription cid)
			throws ConnectorException {
		// TODO Nothing to do
		
	}

}
