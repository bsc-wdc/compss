package integratedtoolkit.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
/**
 * Parameters description
 *
 */
public @interface Parameter {

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

    /**
     * Returns if the parameter has been annotated as an stream entry
     * 
     * @return the stream entry of the parameter
     */
    Stream stream() default Stream.UNSPECIFIED;

}
