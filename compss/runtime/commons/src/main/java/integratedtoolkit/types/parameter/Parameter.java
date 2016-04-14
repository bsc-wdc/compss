package integratedtoolkit.types.parameter;

import java.io.Serializable;
import integratedtoolkit.api.ITExecution.*;


public abstract class Parameter implements Serializable {
	/**
	 * Serializable objects Version UID are 1L in all Runtime
	 */
	private static final long serialVersionUID = 1L;

    // Parameter fields
    private final ParamType type;
    private final ParamDirection direction;

    public Parameter(ParamType type, ParamDirection direction) {
        this.type = type;
        this.direction = direction;
    }

    public ParamType getType() {
        return type;
    }

    public ParamDirection getDirection() {
        return direction;
    }

}
