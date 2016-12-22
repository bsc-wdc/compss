package taskSubmission;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface FaultToleranceItf {

    @Method(declaringClass = "taskSubmission.FaultToleranceImpl")
	void task(
		@Parameter(direction = Direction.IN) int iter
	); 

}
