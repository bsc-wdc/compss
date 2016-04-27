package integratedtoolkit.connectors.one;

import java.util.HashMap;

import org.opennebula.client.Client;
import org.opennebula.client.ClientConfigurationException;
import org.opennebula.client.OneResponse;
import org.opennebula.client.template.Template;
import org.opennebula.client.template.TemplatePool;
import org.opennebula.client.vm.VirtualMachine;

import integratedtoolkit.connectors.AbstractSSHConnector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.connectors.utils.KeyManager;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public class ONE extends AbstractSSHConnector {
	private Client client;
	private static final String ENDPOINT_PROP = "Server";
    private static final String ONE_USER_PROP = "one-user";
    private static final String ONE_PWD_PROP = "one-password";
    private static final String RUNNING = "runn";
    private static final long POLLING_INTERVAL = 5;
    private static final int TIMEOUT = 1800;
	
    public ONE(String providerName, HashMap<String, String> props) throws ConnectorException {
		super(providerName, props);
		try {
            this.client = new Client(props.get(ONE_USER_PROP) + ":" + props.get(ONE_PWD_PROP),
                    props.get(ENDPOINT_PROP));

        } catch (ClientConfigurationException e) {
            logger.error("Error configuring ONE client.", e);
            throw new ConnectorException("Error configuring ONE client.");
        }
	}

	@Override
	public void destroy(Object vm) throws ConnectorException {
		int vmId = Integer.parseInt((String) vm);
        VirtualMachine virtualMachine = new VirtualMachine(vmId, client);
        virtualMachine.shutdown();

	}

	@Override
	public Object create(String name, CloudMethodResourceDescription requested)
			throws ConnectorException {
		Template template = this.classifyMachine(requested.getTotalComputingUnits(),
                requested.getMemorySize(), requested.getStorageSize());

        try {
            String pubKey = KeyManager.getPublicKey(KeyManager.getKeyPair());
            VMTemplate vmTemp = new VMTemplate(template.info().getMessage());
            vmTemp.setImage(requested.getImage().getImageName());
            vmTemp.setPublicKey(pubKey);

            OneResponse resp = template.instantiate(name, false, vmTemp.getString());

            if (resp.isError()) {
                throw new ConnectorException(resp.getErrorMessage());
            }
            return resp.getMessage();

        } catch (ClassCastException e) {
            logger.error("DOM exception when serializing template.", e);
            throw new ConnectorException(e);
        } catch (Exception e) {
            logger.error("Unknown exception.", e);
            throw new ConnectorException(e);
        }
	}

	@Override
	public CloudMethodResourceDescription waitUntilCreation(Object vm,
			CloudMethodResourceDescription requested) throws ConnectorException {
		CloudMethodResourceDescription granted = new CloudMethodResourceDescription();
        int vmId = Integer.parseInt((String) vm);
        VirtualMachine virtualMachine = new VirtualMachine(vmId, client);
        virtualMachine.info();
        int tries = 0;

        while (virtualMachine.status() == null || !virtualMachine.status().equals(RUNNING)) {

            if (tries *  POLLING_INTERVAL > TIMEOUT) {
                throw new ConnectorException("Maximum VM creation time reached.");
            }

            tries++;

            try {
                Thread.sleep(POLLING_INTERVAL * 1000);
            } catch (InterruptedException e) {
                // ignore
            }
            virtualMachine.info();
        }

        String ip = virtualMachine.xpath("//NIC/IP");
        granted.copy(requested);
        granted.setName(ip);
        granted.setOperatingSystemType("Linux");
        granted.setValue(getMachineCostPerTimeSlot(granted));

		return granted;
	}

	@Override
	public float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd) {
		return 0;
	}

	@Override
	public long getTimeSlot() {
		return 0;
	}
	
	@Override
	protected void close(){
	    	//Nothing to do;
	}
	
	private Template classifyMachine(int cpu, float memory, float disk) {
        
        logger.debug("Classifying machine with CPU=" + cpu + ", Mem=" + memory + ", Disk=" + disk);

        // Inputs in MB
        TemplatePool pool = new TemplatePool(this.client);
        int minCPU = Integer.MAX_VALUE;
        float minMem = Float.MAX_VALUE;
        int minDisk = Integer.MAX_VALUE;
        Template minTemplate = null;

        pool.info(); 
        logger.debug("There are a total of " + pool.getLength() + " templates.");
        for (Template temp : pool) {
            temp.info();
            int tmpCPU = Integer.valueOf(temp.xpath("//CPU"));
            float tmpMem = Integer.valueOf(temp.xpath("//MEMORY"));
            int tmpDisk = Integer.valueOf(temp.xpath("//DISK[TYPE=\"fs\"]/SIZE"));

           
            logger.debug("Comparing with template " + temp.getName() + ": CPU=" + tmpCPU
                        + ", Mem=" + tmpMem + ", Disk=" + tmpDisk);

            if (cpu <= tmpCPU && tmpCPU <= minCPU && memory <= tmpMem && tmpMem <= minMem
                    && disk <= tmpDisk && tmpDisk <= minDisk) {
                if (tmpCPU < minCPU || tmpMem < minMem || tmpDisk < minDisk) {
                    minCPU = tmpCPU;
                    minMem = tmpMem;
                    minDisk = tmpDisk;
                    minTemplate = temp;
                }
            }
        }
        if (minTemplate != null) {
            logger.debug("Found a matching template: " + minTemplate.getName());
        }
        return minTemplate;
    }

}
