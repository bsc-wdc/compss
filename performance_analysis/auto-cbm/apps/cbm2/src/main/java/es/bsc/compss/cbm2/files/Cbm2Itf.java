/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
 *
 */
package es.bsc.compss.cbm2.files;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface Cbm2Itf {

    // B = f(A,B)
    @Method(declaringClass = "es.bsc.compss.cbm2.files.Cbm2Impl")
    void runTaskIn(@Parameter(type = Type.INT, direction = Direction.IN) int sleepTime,
        @Parameter(type = Type.FILE, direction = Direction.IN) String dummyFilePath,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String dummyFilePathOut);

    // f(A,B), where B is inout
    @Method(declaringClass = "es.bsc.compss.cbm2.files.Cbm2Impl")
    void runTaskInOut(@Parameter(type = Type.INT, direction = Direction.IN) int sleepTime,
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String dummyFilePath);

}
