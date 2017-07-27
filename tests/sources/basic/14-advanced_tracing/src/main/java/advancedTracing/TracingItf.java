package advancedTracing;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface TracingItf {

    @Method(declaringClass = "advancedTracing.TracingImpl")
    void task1 (
    ); 
    
    @Method(declaringClass = "advancedTracing.TracingImpl")
    void task2 (
    ); 
    
    @Method(declaringClass = "advancedTracing.TracingImpl")
    void task3 (
    ); 

    @Method(declaringClass = "advancedTracing.TracingImpl")
    Integer task4(
        @Parameter(type = Type.FILE, direction = Direction.INOUT)
        String file
    ); 
	
}
