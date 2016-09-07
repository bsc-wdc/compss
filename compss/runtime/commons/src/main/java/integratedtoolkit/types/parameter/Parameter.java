package integratedtoolkit.types.parameter;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.api.COMPSsRuntime.DataType;

import java.io.Serializable;


public abstract class Parameter implements Serializable {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    // Parameter fields
    private DataType type;
    private final DataDirection direction;


    public Parameter(DataType type, DataDirection direction) {
        this.type = type;
        this.direction = direction;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public DataDirection getDirection() {
        return direction;
    }

}
