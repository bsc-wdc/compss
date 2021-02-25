package tracing;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface DemoClassItf {

    @Method(declaringClass = "tracing.DemoClassImpl")
    void task1();

    @Method(declaringClass = "tracing.DemoClassImpl")
    void task2();

    @Method(declaringClass = "tracing.DemoClassImpl")
    void task3();

    @Constraints(computingUnits = "2", memorySize = "200", storageBW = "30")
    @Method(declaringClass = "tracing.DemoClassImpl")
    Integer task4(@Parameter(type = Type.FILE, direction = Direction.INOUT) String file);

}
