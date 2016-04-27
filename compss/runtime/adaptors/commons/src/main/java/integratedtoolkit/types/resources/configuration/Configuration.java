package integratedtoolkit.types.resources.configuration;

import java.util.HashMap;


public class Configuration {
	
	private final String adaptorName;
	private final HashMap<String, String> additionalProperties = new HashMap<String, String> ();
	
	/** 
	 * Instantiates the class
	 * 
	 * @param adaptorName
	 */
	public Configuration(String adaptorName) {
		this.adaptorName = adaptorName;
	}
	
	/**
	 * Returns the adaptor name (fully qualified classname)
	 * 
	 * @return
	 */
	public final String getAdaptorName() {
		return this.adaptorName;
	}
	
	/**
	 * Returns a HashMap with all the additional properties. The keys are the names
	 * of the properties and the values are the values of the properties
	 * 
	 * @return
	 */
	public final HashMap<String, String> getAdditionalProperties() {
		return this.additionalProperties;
	}
	
	/**
	 * Returns the value of the property with name @name. Null if key doesn't exist
	 * 
	 * @param name
	 * @return
	 */
	public final String getProperty(String name) {
		return this.additionalProperties.get(name);
	}
	
	/**
	 * Adds a property with name @name and value @value
	 * 
	 * @param name
	 * @param value
	 */
	public final void addProperty(String name, String value) {
		this.additionalProperties.put(name, value);
	}

}
