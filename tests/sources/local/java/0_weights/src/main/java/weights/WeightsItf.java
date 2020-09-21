package weights;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface WeightsItf {

    @Constraints(computingUnits = "2")
    @Method(declaringClass = "weights.WeightsImpl")
    public void genTask1(@Parameter(type = Type.FILE, direction = Direction.OUT) String filename1,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filename2,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filename3,
        @Parameter(type = Type.STRING, direction = Direction.IN) String content);

    @Constraints(memorySize = "700")
    @Method(declaringClass = "weights.WeightsImpl")
    public void genTask2(@Parameter(type = Type.FILE, direction = Direction.OUT) String filename1,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filename2,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filename3,
        @Parameter(type = Type.STRING, direction = Direction.IN) String content);

    @Method(declaringClass = "weights.WeightsImpl")
    public void readFiles1(@Parameter(type = Type.FILE, direction = Direction.IN, weight = "3.0") String filename1,
        @Parameter(type = Type.FILE, direction = Direction.IN) String filename2,
        @Parameter(type = Type.FILE, direction = Direction.IN) String filename3);

    @Method(declaringClass = "weights.WeightsImpl")
    public void readFiles2(@Parameter(type = Type.FILE, direction = Direction.IN, weight = "3.0") String filename1,
        @Parameter(type = Type.FILE, direction = Direction.IN) String filename2,
        @Parameter(type = Type.FILE, direction = Direction.IN) String filename3);

}
