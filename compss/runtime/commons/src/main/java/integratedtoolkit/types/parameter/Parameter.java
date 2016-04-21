package integratedtoolkit.types.parameter;

import integratedtoolkit.api.ITExecution.ParamDirection;
import integratedtoolkit.api.ITExecution.ParamType;

import java.io.Serializable;


public abstract class Parameter implements Serializable {
	/**
	 * Serializable objects Version UID are 1L in all Runtime
	 */
	private static final long serialVersionUID = 1L;

    // Parameter fields
    private ParamType type;
    private final ParamDirection direction;

    public Parameter(ParamType type, ParamDirection direction) {
        this.type = type;
        this.direction = direction;
    }

    public ParamType getType() {
        return type;
    }

    public void setType(ParamType type) {
        this.type = type;
    }

    public ParamDirection getDirection() {
        return direction;
    }

}
