package integratedtoolkit.types.parameter;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.api.COMPSsRuntime.DataType;


public class SCOParameter extends DependencyParameter {
    /**
	 * Serializable objects Version UID are 1L in all Runtime
	 */
	private static final long serialVersionUID = 1L;
	
	private int code;
	private Object value;

	public SCOParameter(DataType type, DataDirection direction, Object value, int code) {
		 super(type, direction);
		 this.setValue(value);
		 this.setCode(code);
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
	
	public String toString() {
        return "SCO with hash code " + getCode();
    }

}
