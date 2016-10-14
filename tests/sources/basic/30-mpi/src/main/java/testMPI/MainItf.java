package testMPI;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.MPI;


public interface MainItf {
	
	@MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", computingNodes = "1")
	@Constraints(computingUnits = "2")
	public String taskSingleMPI(
		@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data
	);
	
	@MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", computingNodes = "2")
	@Constraints(computingUnits = "2")
	public String taskMultipleMPI(
		@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data
	);

}
