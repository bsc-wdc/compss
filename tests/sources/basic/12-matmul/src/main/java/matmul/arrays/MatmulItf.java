package matmul.arrays;

import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.*;


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
