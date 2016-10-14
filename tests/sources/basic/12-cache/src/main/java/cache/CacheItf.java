package cache;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface CacheItf {
	
	// Project.xml has limit of tasks 1 to ensure only one task is executed at the same time
	
	/*************************************************************************************************
	 * SEND PARAMS METHODS
	 *************************************************************************************************/
	@Method(declaringClass = "cache.CacheImpl")
	Container method (
		@Parameter() int val, 
		@Parameter() boolean bool, 
		@Parameter() String s, 
		@Parameter(type = Type.FILE, direction = Direction.IN) String fileIn, 
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileInOut, 
		@Parameter(type = Type.FILE, direction = Direction.OUT) String fileOut, 
		@Parameter(type = Type.OBJECT, direction = Direction.IN) Container c1, 
		@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Container c2
	);
	
	/*************************************************************************************************
	 * OBJECT CACHE METHODS
	 *************************************************************************************************/
	@Method(declaringClass = "cache.CacheImpl")
	void objectIN(
		@Parameter(type = Type.OBJECT, direction = Direction.IN) Container c
	);
	
	@Method(declaringClass = "cache.CacheImpl")
	void objectINOUT(
		@Parameter(type = Type.OBJECT, direction = Direction.INOUT)	Container c
	);
	
	@Method(declaringClass = "cache.CacheImpl")
	Container objectOUT(
	);
	
	/*************************************************************************************************
	 * FILE CACHE METHODS
	 *************************************************************************************************/	
	@Method(declaringClass = "cache.CacheImpl")
	void fileIN(
		@Parameter(type = Type.FILE, direction = Direction.IN) String fileName
	);

	@Method(declaringClass = "cache.CacheImpl")
	void fileINOUT(
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
	);
	

	@Method(declaringClass = "cache.CacheImpl")
	void fileOUT(
		@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
	);

}
