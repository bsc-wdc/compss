package advancedTracing;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


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
