package integratedtoolkit.connectors.vmm;

import java.util.HashMap;

import integratedtoolkit.ITConstants;
import integratedtoolkit.connectors.AbstractSSHConnector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public class VMMConnector extends AbstractSSHConnector {

    private static final String ENDPOINT_PROP = "Server";
    private static final String ACTIVE = "ACTIVE";
    private static final String ERROR = "ERROR";
    private static final long POLLING_INTERVAL = 5;
    private static final int TIMEOUT = 1800;

    private VMMClient client;


    public VMMConnector(String providerName, HashMap<String, String> props) {
        super(providerName, props);
        this.client = new VMMClient(props.get(ENDPOINT_PROP));
    }

    @Override
    public void destroy(Object vm) throws ConnectorException {
        String vmId = (String) vm;
        try {
            client.deleteVM(vmId);
        } catch (Exception e) {
            logger.error("Exception waiting for VM Creation");
            throw new ConnectorException(e);

        }
    }

    @Override
    public Object create(String name, CloudMethodResourceDescription requested) throws ConnectorException {
        try {
            logger.debug("Image password:" + requested.getImage().getProperties().get(ITConstants.PASSWORD));
            String id = client.createVM(name, requested.getImage().getImageName(), requested.getTotalCPUComputingUnits(),
                    (int) (requested.getMemorySize() * 1000), (int) requested.getStorageSize(),
                    System.getProperty(ITConstants.IT_APP_NAME));
            logger.debug("Machine " + id + " created");
            return id;
        } catch (Exception e) {
            logger.error("Exception submitting vm creation", e);
            throw new ConnectorException(e);
        }
    }

    @Override
    public CloudMethodResourceDescription waitUntilCreation(Object vm, CloudMethodResourceDescription requested) throws ConnectorException {
        CloudMethodResourceDescription granted = new CloudMethodResourceDescription();
        String vmId = (String) vm;
        try {
            VMDescription vmd = client.getVMDescription(vmId);
            logger.info("VM State is " + vmd.getState());
            int tries = 0;
            while (vmd.getState() == null || !vmd.getState().equals(ACTIVE)) {
                if (vmd.getState().equals(ERROR)) {
                    logger.error("Error waiting for VM Creation. Middleware has return an error state");
                    throw new ConnectorException("Error waiting for VM Creation. Middleware has return an error state");
                }
                if (tries * POLLING_INTERVAL > TIMEOUT) {
                    throw new ConnectorException("Maximum VM creation time reached.");
                }

                tries++;

                try {
                    Thread.sleep(POLLING_INTERVAL * 1000);
                } catch (InterruptedException e) {
                    // Ignore
                }
                vmd = client.getVMDescription(vmId);
            }

            granted = requested.copy();
            granted.setName(vmd.getIpAddress());
            granted.setOperatingSystemType("Linux");
            granted.setValue(getMachineCostPerTimeSlot(granted));

            return granted;
        } catch (Exception e) {
            logger.error("Exception waiting for VM Creation");
            throw new ConnectorException(e);
        }

    }

    @Override
    public float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd) {
        return rd.getValue();
    }

    @Override
    public long getTimeSlot() {
        return 0;
    }

    @Override
    protected void close() {
        // Nothing to do;
    }

}
