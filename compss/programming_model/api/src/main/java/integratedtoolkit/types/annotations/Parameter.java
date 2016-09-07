package integratedtoolkit.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
/**
 * Parameters description
 *
 */
public @interface Parameter {

    /**
     * Parameter types
     *
     */
    public static enum Type {
        FILE, 
        BOOLEAN, 
        CHAR, 
        STRING, 
        BYTE, 
        SHORT, 
        INT, 
        LONG, 
        FLOAT, 
        DOUBLE, 
        OBJECT, 
        UNSPECIFIED;
    }

    /**
     * Direction types
     *
     */
    public static enum Direction {
        IN, 
        OUT, 
        INOUT;
    }


    /**
     * Returns the type of the parameter
     * 
     * @return the type of the parameter
     */
    Type type() default Type.UNSPECIFIED;

    /**
     * Returns the direction of the parameter
     * 
     * @return the direction of the parameter
     */
    // Set default direction=IN for basic types
    Direction direction() default Direction.IN;

}
