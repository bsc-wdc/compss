package testMPI;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.MPI;


public interface MainItf {

    @MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", processes = "2", processesPerNode = "2")
    @Constraints(computingUnits = "1")
    int taskSingleMPI(@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = StdIOStream.STDOUT) String fileOut);

    @MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", processes = "2", scaleByCU = true)
    @Constraints(computingUnits = "2")
    Integer taskMultipleMPI(@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = StdIOStream.STDOUT) String fileOut);

    @MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", processes = "2")
    @Constraints(computingUnits = "1")
    Integer taskConcurrentMultipleMPI(@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = StdIOStream.STDOUT) String fileOut);

    @MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", processes = "1")
    @Constraints(computingUnits = "1", processorArchitecture = "amd64")
    Integer taskConcurrentSimple1(@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = StdIOStream.STDOUT) String fileOut);

    @MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", processes = "1", scaleByCU = true)
    @Constraints(computingUnits = "2", processorArchitecture = "amd64")
    Integer taskConcurrentSimple2(@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = StdIOStream.STDOUT) String fileOut);

}
