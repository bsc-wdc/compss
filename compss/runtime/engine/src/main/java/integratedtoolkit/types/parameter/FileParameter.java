package integratedtoolkit.types.parameter;

import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;

import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.parameter.DependencyParameter;


public class FileParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    // File parameter fields
    private DataLocation location;


    public FileParameter(Direction direction, Stream stream, DataLocation location) {
        super(DataType.FILE_T, direction, stream);
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
