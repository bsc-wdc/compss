package tracing;

import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


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
