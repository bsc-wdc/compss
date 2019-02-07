package testConcurrent;

import model.MyFile;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface TestConcurrentItf {

	@Method(declaringClass = "testConcurrent.TestConcurrentImpl", targetDirection="CONCURRENT")
	public void taskPSCOConcurrent(
		@Parameter (type = Type.OBJECT, direction = Direction.CONCURRENT) MyFile f
	);
	
}
