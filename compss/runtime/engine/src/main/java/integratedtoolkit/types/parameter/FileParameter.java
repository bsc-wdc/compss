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
    private final DataLocation location;
    private final String originalName;


    public FileParameter(Direction direction, Stream stream, String prefix, DataLocation location, String originalName) {
        super(DataType.FILE_T, direction, stream, prefix);
        this.location = location;
        this.originalName = originalName;

    }

    public DataLocation getLocation() {
        return location;
    }

    @Override
    public String getOriginalName() {
        return originalName;
    }

    @Override
    public String toString() {
        return "FileParameter with location " + location + ", type " + getType() + ", direction " + getDirection();
    }

}
