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
	
	/*
	@MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", computingNodes = "2")
	@Constraints(computingUnits = "Eval(#par[1]+ size(#stdin)")
	public String taskMultipleMPI(
	    @Parameter(type = Type.INT, direction = Direction.IN, name="val") int val,
		@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
		@Parameter(type = Type.STRING, name = Binary.STDIN) String fileIn,
		@Parameter(type = Type.FILE, direction = INOUT, name = Binary.STDOUT) String fileO,
		@Parameter(type = Type.FILE, name = Binary.STDERR) String fileE,
		@Parameter(type = Type.STRING, name = Binary.STDIN) String fileIn2,
	);
	
	type = STDIN --> file IN
	        STDOUT --> file OUT / INOUT
	        STDERR --> file OUT / INOUT
	
	@MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", computingNodes = "2", stdIn = 2, stdOut = 3, stdErr = 4)
    @Constraints(computingUnits = "Eval(#par[1]+ size(#par[3])")
    public String taskMultipleMPI(
        @Parameter(type = Type.INT, direction = Direction.IN) int val,
        @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
        @Parameter(type = Type.FILE) String fileIn,
        @Parameter(type = Type.FILE) String fileO,
        @Parameter(type = Type.FILE) String fileE,
    );
	       
    PARA HACER: 
	cmd $* < stdIN 1>> stdout 2>> stderr | tee 1> jobN_NEW.out 2> jobN_NEW.err
	
	AHORA SE HACE:
	si la tarea falla o no: $?
	return cmd $*
    */
}
