package testPSCO;

import model.Person;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


public interface MainItf {
	
	@Method(declaringClass = "testPSCO.MainImpl")
	public void taskPSCOIn(
		@Parameter (type = Type.OBJECT, direction = Direction.IN) Person p
	);

	@Method(declaringClass = "testPSCO.MainImpl")
	public void taskPSCOInOut(
		@Parameter (type = Type.OBJECT, direction = Direction.INOUT) Person p
	);

	@Method(declaringClass = "testPSCO.MainImpl")
	public String taskPSCOInOutTaskPersisted(
		@Parameter (type = Type.OBJECT, direction = Direction.INOUT) Person p
	);

	@Method(declaringClass = "testPSCO.MainImpl")
	public Person taskPSCOReturn(
		@Parameter (type = Type.STRING, direction = Direction.IN) String name, 
		@Parameter () int age, 
		@Parameter () int numC,
		@Parameter (type = Type.STRING, direction = Direction.IN) String id
	);

	@Method(declaringClass = "testPSCO.MainImpl")
	public Person taskPSCOReturnNoTaskPersisted(
		@Parameter (type = Type.STRING, direction = Direction.IN) String name, 
		@Parameter () int age, 
		@Parameter () int numC
	);

}
