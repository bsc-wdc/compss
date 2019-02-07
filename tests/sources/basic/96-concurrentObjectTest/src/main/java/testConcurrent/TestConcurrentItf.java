package testConcurrent;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface TestConcurrentItf {

//    @Constraints(computingUnits = "1")
    @Method(declaringClass = "testConcurrent.TestConcurrentImpl")
    void write_one(
        @Parameter(type = Type.FILE, direction = Direction.CONCURRENT) String fileName
    );
    
//    @Constraints(computingUnits = "1")
    @Method(declaringClass = "testConcurrent.TestConcurrentImpl")
    void write_two(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
    
	@Method(declaringClass = "model.MyFile", targetDirection="CONCURRENT")
	public void writeThree(
	);
	
	@Method(declaringClass = "model.MyFile", targetDirection="INOUT")
    public void writeFour(
    );
	
}
