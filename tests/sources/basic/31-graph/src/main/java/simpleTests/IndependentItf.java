package simpleTests;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Method;

import utils.GenericObject;


public interface IndependentItf {
    
    @Method(declaringClass = "utils.TasksImplementation")
    GenericObject initialize(
    );

    @Method(declaringClass = "utils.TasksImplementation")
    void increment(
        @Parameter(type = Type.OBJECT, direction = Direction.INOUT) GenericObject go
    );

}
