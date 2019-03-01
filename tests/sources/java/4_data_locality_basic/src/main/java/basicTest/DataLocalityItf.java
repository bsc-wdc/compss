package basicTest;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface DataLocalityItf {
    
    @Method(declaringClass = "basicTest.DataLocalityImpl")
    void task (
    	@Parameter(type = Type.INT, direction = Direction.IN) int id, 
    	@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
    
}
