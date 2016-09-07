package integratedtoolkit.types.parameter;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.api.COMPSsRuntime.DataType;


public class BasicTypeParameter extends Parameter {

    /*
     * Basic type parameter can be: 
     * - boolean 
     * - char 
     * - String 
     * - byte 
     * - short 
     * - int 
     * - long 
     * - float 
     * - double
     */

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private Object value;


    public BasicTypeParameter(DataType type, DataDirection direction, Object value) {
        super(type, direction);
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value + " " + getType() + " " + getDirection();
    }

}
