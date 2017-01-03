package schedulers;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface MainItf {

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "schedulers.MainImpl")
    void increment(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String file
    );

}
