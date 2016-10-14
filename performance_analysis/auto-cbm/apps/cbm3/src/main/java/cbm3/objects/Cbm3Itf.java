package cbm3.objects;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface Cbm3Itf {
    
	@Method(declaringClass="cbm3.objects.Cbm3")
	DummyPayload runTaskIn(
			@Parameter(type = Type.INT,    direction = Direction.IN) int sleepTime,
			@Parameter(type = Type.OBJECT, direction = Direction.IN) DummyPayload objinLeft,
			@Parameter(type = Type.OBJECT, direction = Direction.IN) DummyPayload objinRight
	);
	
	@Method(declaringClass="cbm3.objects.Cbm3")
	void runTaskInOut(
			@Parameter(type = Type.INT,    direction = Direction.IN)    int sleepTime,
			@Parameter(type = Type.OBJECT, direction = Direction.INOUT) DummyPayload objinoutLeft,
			@Parameter(type = Type.OBJECT, direction = Direction.IN)    DummyPayload objinRight
	);
	
}
