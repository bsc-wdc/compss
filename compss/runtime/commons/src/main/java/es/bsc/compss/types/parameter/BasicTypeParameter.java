package es.bsc.compss.types.parameter;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;


public class BasicTypeParameter extends Parameter {

    /*
     * Basic type parameter can be: 
     * - boolean 
     * - char 
     * - String 
     * - byte 
     * - short 
     * - int 
     * - long 
     * - float 
     * - double
     */

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private Object value;


    public BasicTypeParameter(DataType type, Direction direction, Stream stream, String prefix, Object value) {
        super(type, direction, stream, prefix);
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value + " " + getType() + " " + getDirection();
    }

}
