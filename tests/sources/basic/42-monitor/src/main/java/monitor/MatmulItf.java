package monitor;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface MatmulItf {
	
	@Method(declaringClass = "monitor.MatmulImpl")
	void multiplyAccumulative(
		@Parameter double[] A,
		@Parameter double[] B,
		@Parameter(direction = Direction.INOUT)	double[] C
	);

	@Method(declaringClass = "monitor.MatmulImpl")
	double[] initBlock(
		@Parameter int size
	);

}
