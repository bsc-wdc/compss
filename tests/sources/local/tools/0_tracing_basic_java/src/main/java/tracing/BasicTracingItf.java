package tracing;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface BasicTracingItf {

    @Method(declaringClass = "tracing.BasicTracingImpl")
    void task1();

    @Method(declaringClass = "tracing.BasicTracingImpl")
    void task2();

    @Method(declaringClass = "tracing.BasicTracingImpl")
    void task3();

    @Constraints(computingUnits = "2", memorySize = "200", storageBW = "30")
    @Method(declaringClass = "tracing.BasicTracingImpl")
    Integer task4(@Parameter(type = Type.FILE, direction = Direction.INOUT) String file);

}
