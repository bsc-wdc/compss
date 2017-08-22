package testPSCOExternal;

import model.Person;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


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
