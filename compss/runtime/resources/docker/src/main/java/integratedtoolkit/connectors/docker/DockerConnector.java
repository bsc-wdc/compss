package integratedtoolkit.connectors.docker;

import es.bsc.compss.dockerclient.*;

import java.util.ArrayList;
import java.util.HashMap;

import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DefaultDockerClientConfig.Builder;

import integratedtoolkit.connectors.AbstractSSHConnector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.types.ApplicationPackage;
import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.resources.components.Processor;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.util.ErrorManager;

public class DockerConnector extends AbstractSSHConnector {
	private static final int MIN_STORAGE_SIZE_GB = 16;

	// Open ssh daemon and wait for Master commands
	private static final String[] WORKER_CMD = { "/usr/sbin/sshd", "-D" };

	// This is the user of all the compss/compss base images
	private static final String IMAGE_USERNAME = "compss";
	// The tar.gz must be extracted here (Target dir of the package in the xml)
	private static final String IMAGE_APP_DIR  = "/home/compss"; 


	private DockerClient dockerClient;

	public DockerConnector(String providerName, HashMap<String, String> props) throws Exception {
		super(providerName, props);
		super.setDefaultUser(IMAGE_USERNAME);

		ErrorManager.warn(props.toString());

		// Get properties -----------------------------

		// The host would ideally be the swarm manager. This way, we create
		// containers there, and it schedules them
		// wherever it needs to
		String server = props.get("Server");

		// Must be of the form tcp://1.2.3.4:5678
		// But the Server prop always substitutes tcp by http, so we undo the
		// change
		String host = server.replace("https", "tcp").replace("http", "tcp");
		// ---------------------------------------------

		// Build DockerClient with the specified properties
		Builder b = DefaultDockerClientConfig.createDefaultConfigBuilder();
		b = b.withDockerHost(host); // Host

		DockerClientConfig config = b.build();
		dockerClient = DockerClient.build(config);
	}

	@Override
	public CloudMethodResourceDescription waitUntilCreation(Object _containerId,
			CloudMethodResourceDescription requested) throws ConnectorException {

		// We don't have to wait, since DockerClient is synchronous. By the time
		// this is called, the container resource's been created and started 
		// (if no errors, ofc).

		String containerId = (String) _containerId;
		Container c = dockerClient.getContainerById(containerId);
		if (c == null) {
			String err = "The container " + containerId + " couldn't be retrieved.";
			ErrorManager.warn(err);
			throw new ConnectorException(err);
		}


		CloudMethodResourceDescription granted = requested.copy();
		granted.setOperatingSystemType("Linux");

		String ip = dockerClient.getIpAddress(containerId);
		granted.setName(ip);

		// Set Image and provider properties
		HashMap<String, String> providerProperties = new HashMap<String, String>();
		providerProperties.put("provider-user", IMAGE_USERNAME);
		providerProperties.put("vm-user",       IMAGE_USERNAME);

		// IMAGE DESCRIPTION
		CloudImageDescription imageDescription = granted.getImage();
		
		// Remove COMPSs from the packages list.
		// In Docker images, COMPSs is already installed in the worker.
		// There's no need to transfer/install COMPSs.
		for (ApplicationPackage ap : imageDescription.getPackages()) {
			if (ap.getSource().endsWith("COMPSs.tar.gz")) {
				imageDescription.getPackages().remove(ap);
			}
		}
		//
		
		imageDescription.getConfig().setUser(IMAGE_USERNAME);
		imageDescription.getConfig().setAppDir(IMAGE_APP_DIR);
		granted.setImage(imageDescription);

		// -------------------------------------------------------------

		return granted;
	}

	public void destroy(Object _containerId) throws ConnectorException {
		String containerId = (String) _containerId;
		Container c = dockerClient.getContainerById(containerId);
		dockerClient.removeContainer(c.getId());
	}

	@Override
	/**
	 * We return the containerId
	 */
	public Object create(String name, CloudMethodResourceDescription rd) throws ConnectorException {
		try {
			int[] exposedPorts = new int[1101];
			for (int i = 0; i < 1100; ++i) {
				exposedPorts[i] = 43000 + i;
			}
			exposedPorts[1100] = 22; // SSH
			
			String containerId = 
					dockerClient.createContainer(rd.getImage().getImageName(), 
												 name, 
												 exposedPorts,
												 rd.getTotalCPUComputingUnits(), 
												 rd.getMemorySize(), 
												 WORKER_CMD);
			
			dockerClient.startContainer(containerId);
			return containerId;
		} catch (Exception e) {
			String err = "There was an error creating the container '" + name + "': " + e.getMessage();
			ErrorManager.warn(err);
			throw new ConnectorException(err);
		}
	}

	@Override
	public void close() {
		dockerClient.removeAllContainers();
	}

	@Override
	public float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd) {
		return 0.0f;
	}

	@Override
	public long getTimeSlot() {
		return 0;
	}
}
