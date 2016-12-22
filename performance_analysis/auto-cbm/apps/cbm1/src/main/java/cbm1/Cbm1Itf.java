package cbm1;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface Cbm1Itf {

    @Method(declaringClass = "cbm1.Cbm1Impl")
    String runTaskI(
        @Parameter(type = Type.INT, direction = Direction.IN) int a
    );
    
}
