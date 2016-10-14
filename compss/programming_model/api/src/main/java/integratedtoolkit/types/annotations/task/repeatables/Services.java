package integratedtoolkit.types.annotations.task.repeatables;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import integratedtoolkit.types.annotations.task.Service;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
/**
 * Methods definition
 *
 */
public @interface Services {

    Service[] value();

}
