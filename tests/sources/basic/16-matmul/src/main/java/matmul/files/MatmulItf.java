package matmul.files;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface MatmulItf {
	
	@Method(declaringClass = "matmul.files.MatmulImpl")
	void multiplyAccumulative(
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String file1,
		@Parameter(type = Type.FILE, direction = Direction.IN) String file2,
		@Parameter(type = Type.FILE, direction = Direction.IN) String file3
	);

}
