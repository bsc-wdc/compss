package integratedtoolkit.types.parameter;

import integratedtoolkit.api.ITExecution;
import integratedtoolkit.types.parameter.DependencyParameter;


public class ObjectParameter extends DependencyParameter {
    /**
	 * Serializable objects Version UID are 1L in all Runtime
	 */
	private static final long serialVersionUID = 1L;

    private int hashCode;
    private Object value;

    public ObjectParameter(ITExecution.ParamDirection direction,
            Object value,
            int hashCode) {
        super(ITExecution.ParamType.OBJECT_T, direction);
        this.value = value;
        this.hashCode = hashCode;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getCode() {
        return hashCode;
    }

    public String toString() {
        return "OBJECT: hash code " + hashCode;
    }
}
