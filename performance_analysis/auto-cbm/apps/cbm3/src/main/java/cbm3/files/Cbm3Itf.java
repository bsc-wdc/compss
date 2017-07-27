package cbm3.files;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface Cbm3Itf {
    
	@Method(declaringClass="cbm3.files.Cbm3")
	void runTaskIn(
			@Parameter(type = Type.INT, direction = Direction.IN) int sleepTime,
			@Parameter(type = Type.FILE, direction = Direction.IN) String fileinLeft,
			@Parameter(type = Type.FILE, direction = Direction.IN) String fileinRight,
			@Parameter(type = Type.FILE, direction = Direction.OUT) String fileoutLeft
	);
	
	@Method(declaringClass="cbm3.files.Cbm3")
	void runTaskInOut(
			@Parameter(type = Type.INT, direction = Direction.IN) int sleepTime,
			@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileinoutLeft,
			@Parameter(type = Type.FILE, direction = Direction.IN) String fileinRight
	);
	
}
