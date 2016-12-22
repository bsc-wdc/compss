package tracing;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface TracingItf {

    @Method(declaringClass = "tracing.TracingImpl")
    void task1 (
    ); 
    
    @Method(declaringClass = "tracing.TracingImpl")
    void task2 (
    ); 
    
    @Method(declaringClass = "tracing.TracingImpl")
    void task3 (
    ); 

    @Method(declaringClass = "tracing.TracingImpl")
    Integer task4(
        @Parameter(type = Type.FILE, direction = Direction.INOUT)
        String file
    ); 
	
}
