package matmul.objects;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.task.Method;


public interface MatmulItf {
	
	@Method(declaringClass = "matmul.objects.Block")
	void multiplyAccumulative(
		@Parameter Block a,
		@Parameter Block b
	);
	
}
