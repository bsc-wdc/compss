package integratedtoolkit.types.parameter;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.parameter.DependencyParameter;


public class FileParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    // File parameter fields
    private DataLocation location;


    public FileParameter(DataDirection direction, DataLocation location) {
        super(DataType.FILE_T, direction);
        this.location = location;
    }

    public DataLocation getLocation() {
        return location;
    }

    @Override
    public String toString() {
        return location + " " + getType() + " " + getDirection();
    }
    
}
