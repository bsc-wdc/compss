package testPSCOInternal;

import model.Person;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface InternalItf {
	
	@Method(declaringClass = "testPSCOInternal.InternalImpl")
	public void taskPSCOIn(
		@Parameter (type = Type.OBJECT, direction = Direction.IN) Person p
	);

	@Method(declaringClass = "testPSCOInternal.InternalImpl")
	public void taskPSCOInOut(
		@Parameter (type = Type.OBJECT, direction = Direction.INOUT) Person p
	);

	@Method(declaringClass = "testPSCOInternal.InternalImpl")
	public String taskPSCOInOutTaskPersisted(
		@Parameter (type = Type.OBJECT, direction = Direction.INOUT) Person p
	);

	@Method(declaringClass = "testPSCOInternal.InternalImpl")
	public Person taskPSCOReturn(
		@Parameter (type = Type.STRING, direction = Direction.IN) String name, 
		@Parameter () int age, 
		@Parameter () int numC,
		@Parameter (type = Type.STRING, direction = Direction.IN) String id
	);

	@Method(declaringClass = "testPSCOInternal.InternalImpl")
	public Person taskPSCOReturnNoTaskPersisted(
		@Parameter (type = Type.STRING, direction = Direction.IN) String name, 
		@Parameter () int age, 
		@Parameter () int numC
	);
	
	@Method(declaringClass = "model.Person")
	public void taskPSCOTarget(
	);
	
	@Method(declaringClass = "model.Person")
	public void taskPSCOTargetTaskPersisted(
		@Parameter (type = Type.STRING, direction = Direction.IN) String id
	);
	
	@Method(declaringClass = "testPSCOInternal.InternalImpl")
	public Person taskMap(
		@Parameter (type = Type.STRING, direction = Direction.IN) String newName, 
		@Parameter (type = Type.OBJECT, direction = Direction.IN) Person p2
	);
	
	@Method(declaringClass = "testPSCOInternal.InternalImpl")
	public Person taskReduce(
		@Parameter (type = Type.OBJECT, direction = Direction.IN) Person p1, 
		@Parameter (type = Type.OBJECT, direction = Direction.IN) Person p2
	);
	
	@Method(declaringClass = "model.Person")
    public void taskMap(
        @Parameter (type = Type.STRING, direction = Direction.IN) String newName
    );

	@Method(declaringClass = "model.Person")
    public void taskReduce(
        @Parameter (type = Type.OBJECT, direction = Direction.IN) Person p2
    );
}
