package basicTest;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface DataLocalityItf {
    
    @Method(declaringClass = "basicTest.DataLocalityImpl")
    void task (
    	@Parameter(type = Type.INT, direction = Direction.IN) int id, 
    	@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
    
}
