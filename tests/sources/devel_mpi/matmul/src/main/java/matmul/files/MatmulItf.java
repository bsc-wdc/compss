package matmul.files;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.MPI;


public interface MatmulItf {
	
	@MPI(binary = "${MATMUL_BINARY}", mpiRunner = "mpirun", computingNodes = "2")
	int multiplyAccumulative(
	    @Parameter() int bsize,
		@Parameter(type = Type.FILE, direction = Direction.IN) String aIn,
		@Parameter(type = Type.FILE, direction = Direction.IN) String bIn,
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String cOut
	);

}
