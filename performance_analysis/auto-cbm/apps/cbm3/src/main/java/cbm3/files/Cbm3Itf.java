package cbm3.files;

import integratedtoolkit.types.annotations.*;
import integratedtoolkit.types.annotations.Parameter.*;

public interface Cbm3Itf 
{
	@Method(declaringClass="cbm3.files.Cbm3")
	void runTaskIn
	(
			@Parameter(type = Type.INT, direction = Direction.IN) int sleepTime,
			@Parameter(type = Type.FILE, direction = Direction.IN) String fileinLeft,
			@Parameter(type = Type.FILE, direction = Direction.IN) String fileinRight,
			@Parameter(type = Type.FILE, direction = Direction.OUT) String fileoutLeft
	);
	
	@Method(declaringClass="cbm3.files.Cbm3")
	void runTaskInOut
	(
			@Parameter(type = Type.INT, direction = Direction.IN) int sleepTime,
			@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileinoutLeft,
			@Parameter(type = Type.FILE, direction = Direction.IN) String fileinRight
	);
	
}
