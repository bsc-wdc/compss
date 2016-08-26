package integratedtoolkit.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
/**
 * Methods definition
 *
 */
public @interface Method {

	String[] declaringClass();

	/**
	 * Returns the name of the method
	 * 
	 * @return method's name
	 */
	String name() default Constants.UNASSIGNED_STR;

	/**
	 * Returns whether the method is modifier or not. Avoids synchronization on implicit parameter
	 * 
	 * @return boolean indicating whether the method is modifier or not
	 */
	boolean isModifier() default true;

	/**
	 * Returns if the method has priority or not
	 * 
	 * @return if the method has priority or not
	 */
	boolean priority() default false;

}
