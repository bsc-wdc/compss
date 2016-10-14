package testPSCOExternal;

import model.Person;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface ExternalItf {

	@Method(declaringClass = "model.Person")
	public void taskPSCOTargetWithParams(
		@Parameter (type = Type.STRING, direction = Direction.IN) String newName, 
		@Parameter (type = Type.OBJECT, direction = Direction.IN) Person p
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
