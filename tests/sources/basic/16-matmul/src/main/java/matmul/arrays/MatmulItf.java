package matmul.arrays;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface MatmulItf {
	
	@Method(declaringClass = "matmul.arrays.MatmulImpl")
	void multiplyAccumulative(
		@Parameter double[] A,
		@Parameter double[] B,
		@Parameter(direction = Direction.INOUT)	double[] C
	);

	@Method(declaringClass = "matmul.arrays.MatmulImpl")
	double[] initBlock(
		@Parameter int size
	);

}
