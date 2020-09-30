package objectDeregister;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface ObjectDeregisterItf {

    @Method(declaringClass = "objectDeregister.ObjectDeregisterImpl")
    void task1(@Parameter(type = Type.INT, direction = Direction.IN) int n,
        @Parameter(type = Type.OBJECT, direction = Direction.INOUT) Dummy d1);

    @Method(declaringClass = "objectDeregister.ObjectDeregisterImpl")
    void task2(@Parameter(type = Type.INT, direction = Direction.IN) int n,
        @Parameter(type = Type.OBJECT, direction = Direction.INOUT) Dummy d2,
        @Parameter(type = Type.OBJECT, direction = Direction.IN_DELETE) Dummy din);

    @Method(declaringClass = "objectDeregister.ObjectDeregisterImpl")
    void task3(@Parameter(type = Type.OBJECT, direction = Direction.IN) Dummy d3);

    @Method(declaringClass = "objectDeregister.ObjectDeregisterImpl")
    Dummy task4(@Parameter(type = Type.INT, direction = Direction.IN) int n);

    @Method(declaringClass = "objectDeregister.ObjectDeregisterImpl")
    void task5();

}
