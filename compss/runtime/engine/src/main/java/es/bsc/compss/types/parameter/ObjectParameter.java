package es.bsc.compss.types.parameter;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.parameter.DependencyParameter;


public class ObjectParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private final int hashCode;
    private Object value;


    public ObjectParameter(Direction direction, Stream stream, String prefix, Object value, int hashCode) {
        super(DataType.OBJECT_T, direction, stream, prefix);
        this.value = value;
        this.hashCode = hashCode;
    }

    public Object getValue() {
        return this.value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getCode() {
        return this.hashCode;
    }

    @Override
    public String toString() {
        return "ObjectParameter with hash code " + this.hashCode;
    }

}
