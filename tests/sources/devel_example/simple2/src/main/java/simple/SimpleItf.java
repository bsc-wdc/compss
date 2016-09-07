package simple;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


public interface SimpleItf {

    @Constraints(computingUnits = 1)
    @Method(declaringClass = "simple.SimpleImpl")
    void increment(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String file
    );
    
}
