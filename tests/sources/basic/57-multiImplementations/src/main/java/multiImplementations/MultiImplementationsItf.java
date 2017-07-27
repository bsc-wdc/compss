package multiImplementations;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Binary;
import es.bsc.compss.types.annotations.task.MPI;
import es.bsc.compss.types.annotations.task.Method;


public interface MultiImplementationsItf {
    
    @Constraints(computingUnits = "1")
    @Method(declaringClass = "multiImplementations.Implementation1", constraints = @Constraints(computingUnits = "2"))
    @Method(declaringClass = "multiImplementations.Implementation2")
    void methodMethod(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String counterFile
    );


    @Constraints(computingUnits = "1")
    @Method(declaringClass = "multiImplementations.Implementation1", constraints = @Constraints(computingUnits = "2"))
    @Binary(binary = "${BINARY}")
    void methodBinary(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String counterFile
    );
    
    @Constraints(computingUnits = "1")
    @Method(declaringClass = "multiImplementations.Implementation1", constraints = @Constraints(computingUnits = "2"))
    @Binary(binary = "${BINARY}")
    void binaryMethod(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String counterFile
    );
    
    @Constraints(computingUnits = "1")
    @Method(declaringClass = "multiImplementations.Implementation1", constraints = @Constraints(computingUnits = "2"))
    @MPI(binary = "${MPI_BINARY}", mpiRunner = "mpirun", computingNodes = "1")
    void methodMpi(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String counterFile
    );
    
    @Constraints(computingUnits = "1")
    @Method(declaringClass = "multiImplementations.Implementation1", constraints = @Constraints(computingUnits = "2"))
    @MPI(binary = "${MPI_BINARY}", mpiRunner = "mpirun", computingNodes = "1")
    void mpiMethod(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String counterFile
    );

}
