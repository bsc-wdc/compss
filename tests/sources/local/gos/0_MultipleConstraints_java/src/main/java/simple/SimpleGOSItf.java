package simple;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface SimpleGOSItf {

    @Constraints(appSoftware = "CustomProgram")
    @Method(declaringClass = "simple.SimpleGOSImpl")
    void taskSoftware(@Parameter(type = Type.FILE, direction = Direction.IN) String file);

    @Constraints(processorType = "CPU", computingUnits = "3")
    @Method(declaringClass = "simple.SimpleGOSImpl")
    void taskCPU(@Parameter(type = Type.FILE, direction = Direction.IN) String file);

    @Constraints(processorType = "GPU", computingUnits = "1")
    @Method(declaringClass = "simple.SimpleGOSImpl")
    void taskGPU(@Parameter(type = Type.FILE, direction = Direction.IN) String file);

}
