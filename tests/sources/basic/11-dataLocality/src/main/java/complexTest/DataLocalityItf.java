package complexTest;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface DataLocalityItf {
	
	@Method(declaringClass = "complexTest.Block")
	void multiplyAccumulative(
		@Parameter Block A,
		@Parameter Block B
	);
	
	@Method(declaringClass = "complexTest.Block")
	Block initBlock(
		@Parameter(direction = Direction.IN) int M
	); 
	
}
