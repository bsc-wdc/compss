package integratedtoolkit.types.parameter;

import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;
import integratedtoolkit.types.parameter.DependencyParameter;


public class ObjectParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private int hashCode;
    private Object value;


    public ObjectParameter(Direction direction, Stream stream, String prefix, Object value, int hashCode) {
        super(DataType.OBJECT_T, direction, stream, prefix);
        this.value = value;
        this.hashCode = hashCode;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "OBJECT: hash code " + hashCode;
    }

	@Override
	public String getOriginalName() {
		//Object do not have original name;
		return DependencyParameter.NO_NAME;
	}

}
