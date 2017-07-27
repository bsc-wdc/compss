package matmul.files;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface MatmulItf {
	
	@Method(declaringClass = "matmul.files.MatmulImpl")
	void multiplyAccumulative(
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String file1,
		@Parameter(type = Type.FILE, direction = Direction.IN) String file2,
		@Parameter(type = Type.FILE, direction = Direction.IN) String file3
	);

}
