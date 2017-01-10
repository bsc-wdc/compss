package hidro;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Binary;


public interface HidroItf {
	
	@Binary(binary = "${HIDRO_BINARY}")
	void multiplyAccumulative(
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String file1,
		@Parameter(type = Type.FILE, direction = Direction.IN) String file2,
		@Parameter(type = Type.FILE, direction = Direction.IN) String file3,
		@Parameter() int bsize 
	);

}
