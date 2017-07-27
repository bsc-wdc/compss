package es.bsc.compss.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
/**
 * Orchestration definition
 *
 */
public @interface Orchestration {

    /**
     * Returns the name of the interface
     * 
     * @return the name of the interface
     */
    String interfaceName() default Constants.UNASSIGNED;

}
