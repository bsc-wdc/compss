package integratedtoolkit.connectors.jclouds;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Charsets.UTF_8;

import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Processor;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.Volume;
import org.jclouds.compute.options.TemplateOptions;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;

import es.bsc.jclouds.client.JCloudsClient;
import integratedtoolkit.connectors.AbstractSSHConnector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public class JClouds extends AbstractSSHConnector {

    private JCloudsClient jclouds;
    private String provider;
    private String server;
    private String user;
    private String credential;
    private long timeSlot;
    private static final long POLLING_INTERVAL = 5;
    private static final int TIMEOUT = 1800;
    private static final int DEFAULT_IP_INDEX = 0;
    private int ipIndex = DEFAULT_IP_INDEX;


    public JClouds(String providerName, HashMap<String, String> props) throws Exception {
        super(providerName, props);
        super.setDefaultUser("jclouds");
        server = props.get("Server");
        /*
         * if (server == null) { throw new Exception ("Server endpoint must be specified with \"Server\" property"); }
         */
        provider = props.get("provider");
        if (provider == null) {
            throw new Exception("Provider must be specified with \"provider\" property");
        }
        user = props.get("provider-user");
        if (user == null) {
            throw new Exception("Provider user must be specified with \"provider-user\" property");
        }
        credential = props.get("provider-user-credential");
        if (credential == null) {
            throw new Exception("Provider user credential must be specified with \"provider-user-credential\" property");
        }
        String time = props.get("time-slot");
        if (time != null) {
            timeSlot = Integer.parseInt(time) * 1000;
        } else {
            throw new Exception("Provider billing time-slot must be specified with \"time-slot\" property");
        }
        String index = props.get("ip-index");
        if (index != null) {
            ipIndex = Integer.parseInt(index);
        }

        jclouds = new JCloudsClient(user, credential, provider, server);
    }

    @Override
    public void destroy(Object vm) throws ConnectorException {
        jclouds.destroyNode((String) vm);

    }

    @Override
    public Object create(String name, CloudMethodResourceDescription rd) throws ConnectorException {
        try {
            Template template = generateTemplate(rd);
            Set<? extends NodeMetadata> vms = jclouds.createVMS(name, 1, template);
            return vms.iterator().next().getId();
        } catch (RunNodesException e) {
            throw new ConnectorException(e);
        } catch (IOException e) {
            throw new ConnectorException(e);
        }

    }

    private Template generateTemplate(CloudMethodResourceDescription rd) throws IOException {
        TemplateOptions to = new TemplateOptions();
        // to.overrideLoginUser(super.getDefaultUser());
        String key = super.getKeyPairLocation() + super.getKeyPairName();
        logger.debug("Authorizing keys :" + key);
        to.authorizePublicKey(Files.toString(new File(key + ".pub"), UTF_8));
        to.overrideLoginPrivateKey(Files.toString(new File(key), UTF_8));

        logger.debug("Adding ssh inbound port");
        HashSet<Integer> ports = new HashSet<Integer>();
        ports.add(22);
        MethodConfiguration mc = rd.getImage().getConfig();
        int minPort = mc.getMinPort();
        int maxPort = mc.getMaxPort();
        if (minPort > 0 && maxPort > 0) {
            for (int port = minPort; port < maxPort; ++port) {
                logger.debug("Adding inbound port:" + port);
                ports.add(port);
            }
        }
        to.inboundPorts(Ints.toArray(ports));

        logger.debug("Creating template with image " + rd.getImage().getImageName());
        return jclouds.createTemplate(rd.getType(), rd.getImage().getImageName(), to);
    }

    @Override
    public CloudMethodResourceDescription waitUntilCreation(Object vm, CloudMethodResourceDescription requested) throws ConnectorException {
        CloudMethodResourceDescription granted = new CloudMethodResourceDescription();
        String vmid = (String)vm;
        NodeMetadata vmd = jclouds.getNode(vmid);
        //NodeMetadata vmd = (NodeMetadata) vm;
        try {
            logger.info("VM State is " + vmd.getStatus().toString());
            int tries = 0;
            while (vmd.getStatus() == null || !vmd.getStatus().equals(NodeMetadata.Status.RUNNING)) {

                if (vmd.getStatus().equals(NodeMetadata.Status.ERROR)) {
                    logger.error("Error waiting for VM Creation. Middleware has return an error state");
                    throw new ConnectorException("Error waiting for VM Creation. Middleware has return an error state");
                } else if (vmd.getStatus().equals(NodeMetadata.Status.SUSPENDED)) {
                    logger.error("VM Creation Suspended");
                    throw new ConnectorException("VM creation suspended");
                }
                if (tries * POLLING_INTERVAL > TIMEOUT) {
                    throw new ConnectorException("Maximum VM creation time reached.");
                }

                tries++;

                try {
                    Thread.sleep(POLLING_INTERVAL * 1000);
                } catch (InterruptedException e) {
                    // ignore
                }
                vmd = jclouds.getNode(vmid);
            }
            String ip = getIp(vmd);

            granted = requested.copy();
            granted.setName(ip);
            granted.resetProcessors();

            String arch = CloudMethodResourceDescription.UNASSIGNED_STR;
            List<String> available_archs = requested.getArchitectures();
            if (available_archs != null && !available_archs.isEmpty()) {
                arch = available_archs.get(0);
            }
            for (Processor p : vmd.getHardware().getProcessors()) {
                integratedtoolkit.types.resources.components.Processor runtime_proc = new integratedtoolkit.types.resources.components.Processor();
                runtime_proc.setComputingUnits((int) p.getCores());
                runtime_proc.setSpeed((float) p.getSpeed());
                runtime_proc.setArchitecture(arch);
                granted.addProcessor(runtime_proc);
            }

            granted.setMemorySize(vmd.getHardware().getRam() / 1024);
            float disk = getTotalDisk(vmd.getHardware().getVolumes());
            granted.setStorageSize(disk);
            granted.setOperatingSystemType("Linux");
            granted.setValue(getMachineCostPerTimeSlot(granted));

            return granted;
        } catch (Exception e) {
            logger.error("Exception waiting for VM Creation");
            throw new ConnectorException(e);
        }

    }

    private float getTotalDisk(List<? extends Volume> volumes) {
        float totalDisk = 0;
        for (Volume vol : volumes) {
            if (vol != null) {
                Float size = vol.getSize();
                if (size != null) {
                    totalDisk = totalDisk + size;
                }
            }
        }
        return totalDisk;
    }

    /*
     * private int getTotalCores(List<? extends Processor> processors) { int totalCores = 0; for (Processor proc :
     * processors){ totalCores = totalCores + (int)proc.getCores(); } return totalCores; }
     */

    private String getIp(NodeMetadata vmd) throws ConnectorException {

        if (vmd.getPublicAddresses().isEmpty()) {
            if (vmd.getPrivateAddresses().isEmpty()) {
                throw new ConnectorException("No addresses found in the node description");
            } else {
                if (vmd.getPrivateAddresses().size() < ipIndex + 1) {
                    return vmd.getPrivateAddresses().iterator().next();
                } else {
                    return (String) vmd.getPrivateAddresses().toArray()[ipIndex];
                }
            }
        } else {
            if (vmd.getPublicAddresses().size() < ipIndex + 1) {
                return vmd.getPublicAddresses().iterator().next();
            } else {
                return (String) vmd.getPublicAddresses().toArray()[ipIndex];
            }
        }
    }

    @Override
    public float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd) {
        return rd.getValue();
    }

    @Override
    public long getTimeSlot() {

        return timeSlot;
    }

    @Override
    protected void close() {
        jclouds.close();
    }

}
