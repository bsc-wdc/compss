package basicTypes;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface BasicTypesItf {

	@Method(declaringClass = "basicTypes.BasicTypesImpl")
	void testBasicTypes(
			@Parameter(type = Type.FILE, direction = Direction.OUT) String file,
			@Parameter(type = Type.BOOLEAN) boolean b,
			@Parameter(type = Type.CHAR) char c,
			@Parameter(type = Type.STRING) String s,
			@Parameter(type = Type.BYTE) byte by,
			@Parameter(type = Type.SHORT) short sh,
			@Parameter(type = Type.INT) int i,
			@Parameter(type = Type.LONG, direction = Direction.IN) // Direction not mandatory for basic types, default=IN
																	long l, 
			@Parameter(type = Type.FLOAT) float f,
			@Parameter(type = Type.DOUBLE) double d
	);

}
