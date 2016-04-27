package integratedtoolkit.nio.master.configuration;

import java.io.File;

import integratedtoolkit.nio.master.NIOAdaptor;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;


public class NIOConfiguration extends MethodConfiguration {
	
    private String sandboxWorkingDir;
    private int minPort;
    private int maxPort;
		
    
	public NIOConfiguration(String adaptorName) {
		super(adaptorName);		
	}
	
	@Override
	public void setWorkingDir(String workingDir) {
		super.setWorkingDir(workingDir);
		String sandboxWorkingDir = this.getWorkingDir() + NIOAdaptor.DEPLOYMENT_ID + File.separator + this.getHost() + File.separator;
		this.setSandboxWorkingDir(sandboxWorkingDir);
	}

	public String getSandboxWorkingDir() {
		return sandboxWorkingDir;
	}

	public void setSandboxWorkingDir(String sandboxWorkingDir) {
		this.sandboxWorkingDir = sandboxWorkingDir;
	}

	@Override
	public int getMinPort() {
		return minPort;
	}

	public void setMinPort(int minPort) {
		this.minPort = minPort;
	}

	@Override
	public int getMaxPort() {
		return maxPort;
	}

	public void setMaxPort(int maxPort) {
		this.maxPort = maxPort;
	}
	
}
