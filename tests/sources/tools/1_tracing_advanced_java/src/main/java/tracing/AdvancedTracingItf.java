package tracing;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface AdvancedTracingItf {

    @Method(declaringClass = "tracing.AdvancedTracingImpl")
    void task1 (
    ); 
    
    @Method(declaringClass = "tracing.AdvancedTracingImpl")
    void task2 (
    ); 
    
    @Method(declaringClass = "tracing.AdvancedTracingImpl")
    void task3 (
    ); 

    @Method(declaringClass = "tracing.AdvancedTracingImpl")
    Integer task4(
        @Parameter(type = Type.FILE, direction = Direction.INOUT)
        String file
    ); 
	
}
