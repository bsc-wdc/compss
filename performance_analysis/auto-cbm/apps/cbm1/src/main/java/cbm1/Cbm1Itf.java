package cbm1;

import integratedtoolkit.types.annotations.*;
import integratedtoolkit.types.annotations.Parameter.*;

public interface Cbm1Itf 
{
	@Method(declaringClass = "cbm1.Cbm1Impl")
	String runTaskI
	(
		@Parameter(type = Type.INT, direction = Direction.IN) int a
	);
}
