package simple;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.Processor;
import es.bsc.compss.types.annotations.task.Method;


public interface SimpleGOSItf {

    @Constraints(processorType = "CPU", computingUnits = "2")
    @Method(declaringClass = "simple.SimpleGOSImpl")
    void incrementCPU(@Parameter(type = Type.FILE, direction = Direction.INOUT) String file);

    @Constraints(processorType = "GPU", computingUnits = "1")
    @Method(declaringClass = "simple.SimpleGOSImpl")
    void incrementGPU(@Parameter(type = Type.FILE, direction = Direction.INOUT) String file);

}
