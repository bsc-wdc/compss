package simple;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface SimpleGOSItf {

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "simple.SimpleGOSImpl")
    void increment(@Parameter(type = Type.FILE, direction = Direction.INOUT) String file);

    /*
     * @Method(declaringClass = "simple.SimpleGOSImpl") void increment();
     */

}
