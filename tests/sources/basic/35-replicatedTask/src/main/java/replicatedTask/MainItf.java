package replicatedTask;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import types.Pair;


public interface MainItf {
    
    @Method(declaringClass = "replicatedTask.MainImpl")
    @Constraints(computingUnits = 1)
    void initInitialP(
        @Parameter(type = Type.OBJECT, direction = Direction.INOUT) Pair p
    );

    @Method(declaringClass = "replicatedTask.MainImpl", isReplicated = true)
    @Constraints(computingUnits = 1)
    Pair globalTask(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) Pair p,
        @Parameter(type = Type.INT, direction = Direction.IN) int newX
    );
    
    @Method(declaringClass = "replicatedTask.MainImpl")
    @Constraints(computingUnits = 1)
    void normalTask(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) Pair p
    );

}
