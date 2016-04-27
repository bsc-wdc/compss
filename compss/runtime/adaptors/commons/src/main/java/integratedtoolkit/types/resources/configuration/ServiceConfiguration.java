package integratedtoolkit.types.resources.configuration;


public class ServiceConfiguration extends Configuration {
	
	private final String wsdl;
	private int limitOfTasks;
	
	public ServiceConfiguration(String adaptorName, String wsdl) {
		super(adaptorName);
		this.wsdl = wsdl;
	}

	public String getWsdl() {
		return wsdl;
	}
	
	public int getLimitOfTasks() {
		return limitOfTasks;
	}

	public void setLimitOfTasks(int limitOfTasks) {
		this.limitOfTasks = limitOfTasks;
	}

}
