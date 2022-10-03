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
package testContainer;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Container;


public interface MainItf {

    @Container(engine = "DOCKER", image = "ubuntu", binary = "pwd")
    void pwdEmpty();

    @Container(engine = "DOCKER", image = "ubuntu", binary = "pwd")
    Integer pwdExitValue();

    @Container(engine = "DOCKER", image = "ubuntu", binary = "pwd", workingDir = "${TEST_WORKING_DIR}")
    void pwdWorkingDir();

    @Container(engine = "DOCKER", image = "ubuntu", binary = "${PARAMS_BINARY}", workingDir = "${TEST_WORKING_DIR}/..")
    void customParams(@Parameter() int n, @Parameter() String msg,
        @Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Container(engine = "DOCKER", image = "ubuntu", binary = "pwd", workingDir = "${TEST_WORKING_DIR}")
    void pwdWorkingDirStd(
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = StdIOStream.STDOUT) String stdout,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = StdIOStream.STDERR) String stderr);

    @Container(engine = "DOCKER", image = "ubuntu", options = "-e HOLA=hola", binary = "env", workingDir = "${TEST_WORKING_DIR}")
    void options();
}
