package objectTest;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface ObjectTestItf {

	@Method(declaringClass = "objectTest.ObjectTestImpl")
	void printObjects(
		@Parameter(type = Type.OBJECT, direction = Direction.IN)
		A a,
		@Parameter(type = Type.OBJECT, direction = Direction.IN)
		B b
	);
	
	@Method(declaringClass = "objectTest.ObjectTestImpl")
	void updateObjects(
		@Parameter(type = Type.OBJECT, direction = Direction.INOUT)
		A a,
		@Parameter(type = Type.OBJECT, direction = Direction.INOUT)
		B b,
		@Parameter(type = Type.INT, direction = Direction.IN)
		int i,
		@Parameter(type = Type.OBJECT, direction = Direction.IN)
		String s
	);
	
	@Method(declaringClass = "objectTest.ObjectTestImpl")
	C createCObject(
		@Parameter(type = Type.INT, direction = Direction.IN)
		int i,
		@Parameter(type = Type.OBJECT, direction = Direction.IN)
		String s
	);
	
	@Method(declaringClass = "objectTest.ObjectTestImpl")
	Integer createInteger(
		@Parameter(type = Type.INT, direction = Direction.IN)
		int i
	);
	
	@Method(declaringClass = "objectTest.ObjectTestImpl")
	String createString(
		@Parameter(type = Type.OBJECT, direction = Direction.IN)
		String s
	);
	
	@Method(declaringClass = "objectTest.ObjectTestImpl")
	void printContent(
		@Parameter(type = Type.OBJECT, direction = Direction.IN)
		Integer i,
		@Parameter(type = Type.OBJECT, direction = Direction.IN)
		String s
	);
	
	@Method(declaringClass = "objectTest.A")
	void setIntField(
		@Parameter(type = Type.INT, direction = Direction.IN)
		int i
	);
	
	@Method(declaringClass = "objectTest.A", isModifier = true)
	Integer getAndSetIntField(
		@Parameter(type = Type.INT, direction = Direction.IN)
		int i
	);
	
	@Method(declaringClass = "objectTest.B")
	void setStringField(
		@Parameter(type = Type.STRING, direction = Direction.IN)
		String s
	);
	
	@Method(declaringClass = "objectTest.A")
	int square();
	
	@Method(declaringClass = "objectTest.ObjectTestImpl")
	int[] createIntArray(
		@Parameter(type = Type.INT, direction = Direction.IN)
		int i
	);
	
	@Method(declaringClass = "objectTest.ObjectTestImpl")
	A[][] createObjectArray(	
		@Parameter(type = Type.INT, direction = Direction.IN)
		int i
	);
	
	@Method(declaringClass = "objectTest.ObjectTestImpl")
	void printArrays(
		@Parameter(type = Type.OBJECT, direction = Direction.IN)
		int[] array1,
		@Parameter(type = Type.OBJECT, direction = Direction.IN)
		A[][] array2
	);
	
}
