package integratedtoolkit.types.annotations.task;

import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.task.repeatables.Decafs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(Decafs.class)
public @interface Decaf {

	    /**
	     * Returns the decaf data-flow generation script
	     * 
	     * @return the binary name
	     */
	    String dfScript() default Constants.UNASSIGNED;

	    
	    /**
	     * Returns the decaf dataflow executor
	     * 
	     * @return the binary name
	     */
	    String dfExecutor() default Constants.UNASSIGNED;
	    
	    /**
	     * Returns the decaf dataflow lib
	     * 
	     * @return the binary name
	     */
	    String dfLib() default Constants.UNASSIGNED;
	    
	    /**
	     * Returns the working directory 
	     * 
	     * @return the binary working directory
	     */
	    String workingDir() default Constants.UNASSIGNED;

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
