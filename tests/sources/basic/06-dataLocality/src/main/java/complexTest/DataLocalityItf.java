package complexTest;

import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;


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
