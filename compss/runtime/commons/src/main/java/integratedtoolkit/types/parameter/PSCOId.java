package integratedtoolkit.types.parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class PSCOId implements Serializable {
    /**
	 * Serializable objects Version UID are 1L in all Runtime
	 */
	private static final long serialVersionUID = 1L;
	
	private int hashCode;
	private String id;
	private List<String> backends;
	
	public PSCOId() {
	}
	
	public PSCOId(Object o, String id) {
		this.hashCode = o.hashCode();
		this.setId(id);
		this.setBackends(new ArrayList<String>());
	}
	
	public int hashCode() {
		return hashCode;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public String toString() {
		return id;
	}

	public List<String> getBackends() {
		return backends;
	}

	public void setBackends(List<String> backends) {
		this.backends = backends;
	}
}
