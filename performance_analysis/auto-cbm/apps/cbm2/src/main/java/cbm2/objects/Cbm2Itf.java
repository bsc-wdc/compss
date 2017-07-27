package cbm2.objects;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface Cbm2Itf {
    
	@Method(declaringClass = "cbm2.objects.Cbm2Impl")
	DummyPayload runTaskIn //B = f(A,B)
	(
		@Parameter(type = Type.INT, direction = Direction.IN)   int sleepTime,
		@Parameter(type = Type.OBJECT, direction = Direction.IN)  DummyPayload dummyIn
	);

	@Method(declaringClass = "cbm2.objects.Cbm2Impl")
	void runTaskInOut //f(A,B), where B is inout
	(
		@Parameter(type = Type.INT, direction = Direction.IN) int sleepTime,
		@Parameter(type = Type.OBJECT, direction = Direction.INOUT) DummyPayload dummy
	);
	
}