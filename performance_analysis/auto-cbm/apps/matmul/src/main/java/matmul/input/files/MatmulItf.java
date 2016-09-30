package matmul.input.files;

import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


public interface MatmulItf {
	
	@Method(declaringClass = "matmul.input.files.MatmulImpl")
	void multiplyAccumulative(
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String file1,
		@Parameter(type = Type.FILE, direction = Direction.IN) String file2,
		@Parameter(type = Type.FILE, direction = Direction.IN) String file3
	);

}
