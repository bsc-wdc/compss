package integratedtoolkit.types.parameter;

import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;
import integratedtoolkit.types.parameter.DependencyParameter;


public class ExternalObjectParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private final int hashCode;
    private String pscoId;


    public ExternalObjectParameter(Direction direction, Stream stream, String prefix, String pscoId, int hashCode) {
        super(DataType.EXTERNAL_OBJECT_T, direction, stream, prefix);
        this.pscoId = pscoId;
        this.hashCode = hashCode;
    }

    public String getId() {
        return this.pscoId;
    }

    public void setId(String pscoId) {
        this.pscoId = pscoId;
    }

    public int getCode() {
        return this.hashCode;
    }

    @Override
    public String toString() {
        return "ExternalObjectParameter with Id " + this.pscoId + " and HashCode " + this.hashCode;
    }

}
