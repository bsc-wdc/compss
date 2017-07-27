package es.bsc.compss.types.annotations.task;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.task.repeatables.Services;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(Services.class)
/**
 * Services definition
 *
 */
public @interface Service {

    /*
     * SERVICE DEFINITION
     */

    /**
     * Returns the namespace of the Service
     * 
     * @return the namespace of the Service
     */
    String namespace();

    /**
     * Returns the name of the Service
     * 
     * @return the name of the Service
     */
    String name();

    /*
     * SERVICE PROPERTIES
     * 
     */

    /**
     * Returns the port of the Service
     * 
     * @return the port of the Service
     */
    String port();

    /**
     * Returns the operation name of the Service
     * 
     * @return the operation name of the Service
     */
    String operation() default Constants.UNASSIGNED;

    /*
     * COMMON PROPERTIES
     * 
     */

    /**
     * Returns if the method has priority or not
     * 
     * @return if the method has priority or not
     */
    String priority() default Constants.IS_NOT_PRIORITARY_TASK;

}
