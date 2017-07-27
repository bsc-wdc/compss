package cbm1;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface Cbm1Itf {

    @Method(declaringClass = "cbm1.Cbm1Impl")
    String runTaskI(
        @Parameter(type = Type.INT, direction = Direction.IN) int a
    );
    
}
