package taskSubmission;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface FaultToleranceItf {

    @Method(declaringClass = "taskSubmission.FaultToleranceImpl")
	void task(
		@Parameter(direction = Direction.IN) int iter
	); 

}
