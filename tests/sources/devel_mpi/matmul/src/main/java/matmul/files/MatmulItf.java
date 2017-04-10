package matmul.files;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.MPI;
import integratedtoolkit.types.annotations.task.Method;


public interface MatmulItf {
    
    @Method(declaringClass = "matmul.files.MatmulImpl")
    @Constraints(computingUnits = "1")
    void initializeBlock(
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filename, 
        @Parameter() int BSIZE, 
        @Parameter() boolean initRand
    );

    @Method(declaringClass = "matmul.files.MatmulImpl")
    @Constraints(computingUnits = "${CUS}")
    Integer multiplyAccumulativeNative(
        @Parameter() int bsize, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String aIn,
        @Parameter(type = Type.FILE, direction = Direction.IN) String bIn,
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String cOut
    );
    
    @MPI(binary = "${MATMUL_BINARY}", 
         mpiRunner = "mpirun", 
         computingNodes = "${MPI_PROCS}")
    @Constraints(computingUnits = "${CUS}")
    Integer multiplyAccumulativeMPI(
       @Parameter() int bsize, 
       @Parameter(type = Type.FILE, direction = Direction.IN) String aIn,
       @Parameter(type = Type.FILE, direction = Direction.IN) String bIn,
       @Parameter(type = Type.FILE, direction = Direction.INOUT) String cOut
    );

}
