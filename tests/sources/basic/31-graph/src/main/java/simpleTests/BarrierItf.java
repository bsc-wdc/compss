package simpleTests;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;

import utils.GenericObject;


public interface BarrierItf {
    
    @Method(declaringClass = "utils.TasksImplementation")
    GenericObject initialize(
    );

    @Method(declaringClass = "utils.TasksImplementation")
    void increment(
        @Parameter(type = Type.OBJECT, direction = Direction.INOUT) GenericObject go
    );

}
