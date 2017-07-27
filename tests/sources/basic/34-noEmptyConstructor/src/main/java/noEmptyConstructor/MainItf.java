package noEmptyConstructor;

import customObjectClasses.InvalidObject;
import customObjectClasses.ValidObject;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface MainItf {
    
    @Method(declaringClass = "noEmptyConstructor.MainImpl")
    ValidObject validTask(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) ValidObject voIN,
        @Parameter(type = Type.OBJECT, direction = Direction.INOUT) ValidObject voINOUT
    );
    
    @Method(declaringClass = "noEmptyConstructor.MainImpl")
    InvalidObject invalidTask(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) InvalidObject invoIN,
        @Parameter(type = Type.OBJECT, direction = Direction.INOUT) InvalidObject invoINOUT
    );

}
