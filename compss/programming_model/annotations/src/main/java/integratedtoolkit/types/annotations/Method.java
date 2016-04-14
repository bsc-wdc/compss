package integratedtoolkit.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Method {

    static final String UNASSIGNED = "[unassigned]";

    String[] declaringClass();

    String name() default UNASSIGNED;

    boolean isModifier() default true;

    // NOT CURRENTLY TREATED INSIDE THE RUNTIME
    boolean isInit() default true;

    // NOT CURRENTLY TREATED INSIDE THE RUNTIME
    boolean isParallel() default false;

    boolean priority() default false;
}
