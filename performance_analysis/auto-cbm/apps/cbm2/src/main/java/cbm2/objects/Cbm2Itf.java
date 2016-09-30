package cbm2.objects;

import integratedtoolkit.types.annotations.Parameter.*;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.*;

public interface Cbm2Itf 
{
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