package integratedtoolkit.types.annotations.task;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.task.repeatables.OpenCLs;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(OpenCLs.class)
/**
 * Methods definition
 *
 */
public @interface OpenCL {

    /*
     * OpenCL DEFINITION
     * 
     */

    /**
     * Returns the kernel name
     * 
     * @return the kernel name
     */
    String kernel() default Constants.UNASSIGNED;

    /*
     * COMMON PROPERTIES
     * 
     */

    /**
     * Returns if the method has priority or not
     * 
     * @return if the method has priority or not
     */
    boolean priority() default !Constants.PRIORITY;

    /**
     * Returns the method specific constraints
     * 
     * @return the method specific constraints
     */
    Constraints constraints() default @Constraints();

}
