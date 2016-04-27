package matmul.objects;

import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;


public interface MatmulItf {
	
	@Method(declaringClass = "matmul.objects.Block")
	void multiplyAccumulative(
		@Parameter Block a,
		@Parameter Block b
	);
	
}
