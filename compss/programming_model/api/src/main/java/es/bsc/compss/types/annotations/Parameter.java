package es.bsc.compss.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;


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

    /**
     * Returns the prefix of the parameter
     * 
     * @return the prefix of the parameter
     */
    String prefix() default Constants.PREFIX_EMTPY;

}
