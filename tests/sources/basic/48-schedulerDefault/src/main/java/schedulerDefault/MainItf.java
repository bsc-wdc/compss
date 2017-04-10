package schedulerDefault;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface MainItf {
    
    @Constraints(computingUnits = "1")
    @Method(declaringClass = "schedulerDefault.MainImpl")
    void increment(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileInOut,
        @Parameter(type = Type.FILE, direction = Direction.IN) String fileIn
    );

}
