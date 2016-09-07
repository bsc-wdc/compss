package integratedtoolkit.types.parameter;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.parameter.DependencyParameter;


public class ObjectParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private int hashCode;
    private Object value;


    public ObjectParameter(DataDirection direction, Object value, int hashCode) {
        super(DataType.OBJECT_T, direction);
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

    @Override
    public String toString() {
        return "OBJECT: hash code " + hashCode;
    }

}
