/*
 *  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package blast;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface BlastItf {

	@Method(declaringClass = "blast.BlastImpl")
	@Constraints(computingUnits = "2")
	void align(
		@Parameter(type = Type.STRING, direction = Direction.IN) String databasePath,
		@Parameter(type = Type.FILE, direction = Direction.IN) String partitionFile,
		@Parameter(type = Type.FILE, direction = Direction.OUT) String partitionOutput,
		@Parameter(type = Type.STRING, direction = Direction.IN) String blastBinary,
		@Parameter(type = Type.STRING, direction = Direction.IN) String commandArgs
	);

	@Method(declaringClass = "blast.BlastImpl")
	@Constraints(computingUnits = "2")
	void assemblyPartitions(
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String partialFileA,
		@Parameter(type = Type.FILE, direction = Direction.IN) String partialFileB
	);
}
