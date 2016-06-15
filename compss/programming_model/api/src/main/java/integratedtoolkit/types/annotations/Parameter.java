package integratedtoolkit.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Parameter {

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

    public static enum Direction {
        IN,
        OUT,
        INOUT;
    }

    Type type() default Type.UNSPECIFIED;

    // Set default direction=IN for basic types
    Direction direction() default Direction.IN;

}
