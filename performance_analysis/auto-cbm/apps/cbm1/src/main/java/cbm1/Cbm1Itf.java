package cbm1;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface Cbm1Itf {

    @Method(declaringClass = "cbm1.Cbm1Impl")
    String runTaskI(
        @Parameter(type = Type.INT, direction = Direction.IN) int a
    );
    
}
