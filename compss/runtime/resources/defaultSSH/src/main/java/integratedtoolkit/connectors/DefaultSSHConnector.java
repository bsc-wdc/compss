package integratedtoolkit.connectors;

import es.bsc.conn.Connector;
import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.Processor;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.VirtualResource;

import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


public class DefaultSSHConnector extends AbstractSSHConnector {

    private static final String CONNECTOR_CLASS = "connector-class";

    private static final String CPU_TYPE = "CPU";
    private static final float UNASSIGNED_FLOAT = -1.0f;

    private Connector connector;

    private Processor getProcessor(integratedtoolkit.types.resources.components.Processor p) {
        return new Processor(
                p.getName(),
                p.getComputingUnits(),
                p.getSpeed(),
                p.getArchitecture(),
                p.getPropName(),
                p.getPropValue()
        );
    }

    private integratedtoolkit.types.resources.components.Processor getProcessor(Processor p) {
        // FIXME Assuming that all processors have type CPU and no mem
        return new integratedtoolkit.types.resources.components.Processor(
               p.getName(),
               p.getComputingUnits(),
               p.getSpeed(),
               p.getArchitecture(),
               CPU_TYPE,
               UNASSIGNED_FLOAT,
               p.getPropName(),
               p.getPropValue()
       );
    }

    private List<Processor> getConnectorProcessors(List<integratedtoolkit.types.resources.components.Processor> processorList) {
        List<Processor> processors = new LinkedList<Processor>();
        for (integratedtoolkit.types.resources.components.Processor p: processorList) {
            processors.add(getProcessor(p));
        }
        return processors;
    }

    private List<integratedtoolkit.types.resources.components.Processor> getCompssProcessors(List<Processor> processorList) {
        List<integratedtoolkit.types.resources.components.Processor> processors = new
                LinkedList<integratedtoolkit.types.resources.components.Processor>();

        for (Processor p: processorList) {
            processors.add(getProcessor(p));
        }
        return processors;
    }

    private HardwareDescription getHardwareDescription(CloudMethodResourceDescription cmrd) {
        // FIXME Using CPU Computing units, should check all units
        return new HardwareDescription(
                getConnectorProcessors(cmrd.getProcessors()),
                cmrd.getTotalCPUComputingUnits(),
                cmrd.getMemorySize(),
                cmrd.getMemoryType(),
                cmrd.getStorageSize(),
                cmrd.getStorageType(),
                cmrd.getPriceTimeUnit(),
                cmrd.getPricePerUnit(),
                cmrd.getImage().getImageName(),
                cmrd.getType(),
                cmrd.getImage().getProperties()
        );
    }

    private SoftwareDescription getSoftwareDescription(CloudMethodResourceDescription cmrd) {
        return new SoftwareDescription(
                cmrd.getOperatingSystemType(),
                cmrd.getOperatingSystemDistribution(),
                cmrd.getOperatingSystemVersion(),
                cmrd.getImage().getImageName(),
                cmrd.getType(),
                cmrd.getImage().getProperties(),
                cmrd.getAppSoftware()
        );
    }

    private VirtualResource getVirtualResource(Object id, CloudMethodResourceDescription cmrd) {
        return new VirtualResource(
                (String) id,
                getHardwareDescription(cmrd),
                getSoftwareDescription(cmrd),
                cmrd.getImage().getProperties()
        );
    }

    private CloudImageDescription getCloudImageDescription(HardwareDescription hd,
            SoftwareDescription sd, String provider) {
        CloudImageDescription cid = new CloudImageDescription(
                provider,
                hd.getImageName(),
                hd.getImageProperties());
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

    private CloudMethodResourceDescription toCloudMethodResourceDescription(VirtualResource vr,
            CloudMethodResourceDescription requested) {
        String provider = requested.getProviderName();
        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(
                provider,
                vr.getIp(),
                vr.getHd().getImageName());
        setHardwareInResourceDescription(cmrd, vr.getHd());
        setSoftwareInResourceDescription(cmrd, vr.getSd());
        cmrd.setImage(getCloudImageDescription(vr.getHd(), vr.getSd(), provider));
        setCloudImageDescription(cmrd.getImage(), requested.getImage());
        return cmrd;
    }

    public DefaultSSHConnector(String providerName, HashMap<String, String> props) throws Exception {
        super(providerName, props);
        super.setDefaultUser("");

        String connectorPath = props.get(CONNECTOR_CLASS);

        logger.debug("Using connector " + connectorPath);

        Class<?> conClass = Class.forName(connectorPath);
        Constructor<?> constructor = conClass.getDeclaredConstructors()[0];
        connector = (Connector) constructor.newInstance(props);
    }

    @Override
    public void destroy(Object id) throws ConnectorException {
        logger.debug("Destroy connection with id " + id);
        connector.destroy(id);
    }

    @Override
    public Object create(String name, CloudMethodResourceDescription cmrd) throws ConnectorException {
        logger.debug("Create connection " + name);
        Object created;
        try {
            created = connector.create(
                    getHardwareDescription(cmrd),
                    getSoftwareDescription(cmrd),
                    cmrd.getImage().getProperties());
        } catch (es.bsc.conn.exceptions.ConnectorException ce) {
            throw new ConnectorException(ce);
        }
        return created;
    }

    @Override
    public CloudMethodResourceDescription waitUntilCreation(Object id, CloudMethodResourceDescription requested) throws ConnectorException {
        logger.debug("Waiting for " + (String) id);
        VirtualResource vr;
        try {
            vr = connector.waitUntilCreation(id);
        } catch (es.bsc.conn.exceptions.ConnectorException ce) {
            throw new ConnectorException(ce);
        }
        CloudMethodResourceDescription cmrd = toCloudMethodResourceDescription(vr, requested);
        logger.debug("Return cloud method resource description " + cmrd.toString());
        return cmrd;
    }

    @Override
    public float getMachineCostPerTimeSlot(CloudMethodResourceDescription cmrd) {
        return connector.getPriceSlot(getVirtualResource("-1", cmrd));
    }

    @Override
    public long getTimeSlot() {
        return connector.getTimeSlot();
    }

    @Override
    protected void close() {
        logger.debug("Close connector");
        connector.close();
    }

}
