package es.bsc.compss.dockerconnector;

import java.util.HashMap;
import java.util.List;

import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DefaultDockerClientConfig.Builder;

import es.bsc.compss.dockerclient.*;
import integratedtoolkit.connectors.AbstractSSHConnector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.util.ErrorManager;

public class DockerConnector extends AbstractSSHConnector
{
        private static final String WORKER_CMD = "sshd -D";

        private DockerClient dockerClient;

        /**
         * Creates a DockerConnector to a host without TLS
         * @param host The host url. Must have this format: tcp://<ip>:<port>
         */
        public DockerConnector(String host) {
        	this(host, "");
        }

        /**
         * Creates a DockerConnector to a host with TLS.
         * @param host The host url. Must have this format: tcp://<ip>:<port>
         * @param tlsCertPath The path to the TLS certificate.
         */
        public DockerConnector(String host, String tlsCertPath) {
        	super(host, new HashMap<String, String>()); //Null properties by default
    		super.setDefaultUser("root"); // This is the user of the compss/compss images
        	
        	Builder b = DefaultDockerClientConfig.createDefaultConfigBuilder();
        	b = b.withDockerHost(host);
	        
	        if (!tlsCertPath.isEmpty()) {
	        	b =  b.withDockerTlsVerify(true)
	                  .withDockerCertPath(tlsCertPath);
	        }
        	
	        DockerClientConfig config = b.build();
	        dockerClient = DockerClient.build(config);
        }

        @Override
        public float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd) {
                return 0.0f;
        }

        @Override
        public long getTimeSlot() {
                return 0;
        }

        @Override
        public CloudMethodResourceDescription waitUntilCreation(
        		Object _containerId,
                CloudMethodResourceDescription requested) throws ConnectorException {
        	
        	// We don't have to wait, since DockerClient is synchronous. By the time this is called,
        	// the container resource is created (if there weren't any errors ofc).
        	
        	String containerId = (String) _containerId;
        	Container c = dockerClient.getContainerByName(containerId);
        	if (c == null) {
        		throw new ConnectorException("The container " + containerId + " couldn't be created.");
        	}
        	
        	// Get container ip (to do this we will use its first network).
        	ContainerNetwork[] containerNetworks = 
        			(ContainerNetwork[]) c.getNetworkSettings().getNetworks().values().toArray();
        	if (containerNetworks.length == 0) {
        		throw new ConnectorException("The container " + containerId + " has no networks associated.");
        	}
        	ContainerNetwork net = containerNetworks[0];
        	String ip = net.getIpAddress();
        	
        	// Fill up CloudMethodResourceDescription
        	CloudMethodResourceDescription granted = requested.copy();
        	granted.setName(ip);
        	granted.setPricePerUnit(0.0f);
        	granted.setPriceTimeUnit(0);
        	// granted.setMemorySize(...);
        	// granted.setProcessors(...);
        	
        	CloudImageDescription imageDescription = 
        			new CloudImageDescription("", c.getImage(), new HashMap<String, String>());
        	granted.setImage(imageDescription);
        	
            return granted;
        }

		public void destroy(Object container) throws ConnectorException {
            Container c = (Container) container;
            dockerClient.removeContainer(c.getId());
		}

        @Override
        public Object create(String name, CloudMethodResourceDescription rd) throws ConnectorException {
        	
        	// We return the containerId
        	
        	try {
				String containerId = dockerClient.createContainer("compss/compss", name, WORKER_CMD);
				return containerId;
			} catch (Exception e) {
				throw new ConnectorException("There was an error creating the container '" + name + "'");
			}
        }

        @Override
        public void close() {
                dockerClient.stopAllContainers();
        }
}
