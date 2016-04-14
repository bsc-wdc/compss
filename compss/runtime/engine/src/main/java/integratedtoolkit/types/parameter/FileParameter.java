package integratedtoolkit.types.parameter;

import integratedtoolkit.api.ITExecution;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.parameter.DependencyParameter;


public class FileParameter extends DependencyParameter {
    /**
	 * Serializable objects Version UID are 1L in all Runtime
	 */
	private static final long serialVersionUID = 1L;
	
    // File parameter fields
    private DataLocation location;

    public FileParameter(ITExecution.ParamDirection direction, DataLocation location) {

        super(ITExecution.ParamType.FILE_T, direction);
        this.location = location;
    }

    public DataLocation getLocation() {
        return location;
    }

    public String toString() {
        return location + " "
                + getType() + " "
                + getDirection();
    }
}
