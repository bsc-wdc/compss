package taskSubmission;

import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;


public interface FaultToleranceItf {

    @Method(declaringClass = "taskSubmission.FaultToleranceImpl")
	void task(
		@Parameter(direction = Direction.IN) int iter
	); 

}
