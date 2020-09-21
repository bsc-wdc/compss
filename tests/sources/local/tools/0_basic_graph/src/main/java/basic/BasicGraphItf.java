package basic;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface BasicGraphItf {

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "basic.BasicGraphImpl")
    void inTask();

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "basic.BasicGraphImpl")
    void inTask(@Parameter() boolean token);

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "basic.BasicGraphImpl")
    void inTask(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer token);

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "basic.BasicGraphImpl")
    void inoutTask(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer token);

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "basic.BasicGraphImpl")
    Integer outTask();
}
