package complexTest;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


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
