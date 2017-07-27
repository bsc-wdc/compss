package cbm3.objects;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


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
