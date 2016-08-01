package integratedtoolkit.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
/**
 * Services definition
 *
 */
public @interface Service {

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
    String operation() default Constants.UNASSIGNED_STR;

    /**
     * Returns if the method has priority or not
     * 
     * @return if the method has priority or not
     */
    boolean priority() default false;

}
