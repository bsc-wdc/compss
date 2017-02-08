package integratedtoolkit.types.annotations.task;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.task.repeatables.MPIs;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(MPIs.class)
/**
 * Methods definition
 *
 */
public @interface MPI {

    /*
     * MPI DEFINITION
     * 
     */

    /**
     * Returns the binary name
     * 
     * @return the binary name
     */
    String binary() default Constants.UNASSIGNED;

    /*
     * MPI PROPERTIES
     * 
     */

    /**
     * Returns the mpi runner
     * 
     * @return the mpi runner
     */
    String mpiRunner() default Constants.UNASSIGNED;

    /**
     * Returns the number of computing nodes required
     * 
     * @return the number of computing nodes required
     */
    String computingNodes() default Constants.UNASSIGNED;

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

    /**
     * Returns the method specific constraints
     * 
     * @return the method specific constraints
     */
    Constraints constraints() default @Constraints();

}
