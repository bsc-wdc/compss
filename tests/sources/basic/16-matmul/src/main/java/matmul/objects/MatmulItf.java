package matmul.objects;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.task.Method;


public interface MatmulItf {
	
	@Method(declaringClass = "matmul.objects.Block")
	void multiplyAccumulative(
		@Parameter Block a,
		@Parameter Block b
	);
	
}
