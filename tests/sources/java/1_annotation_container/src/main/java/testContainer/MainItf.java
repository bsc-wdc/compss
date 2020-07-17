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
import es.bsc.compss.types.annotations.task.Binary;
import es.bsc.compss.types.annotations.task.Container;


public interface MainItf {

    // @Binary(binary="/test/ejecutable/exec", workingDir="/test", container = @BinaryContainer(engine="SINGULARITY",
    // image="/home/compss/singularity/examples/ubuntu_latest.sif"))
    // @Binary(binary="/test/ejecutable/exec", workingDir="/test", container = @BinaryContainer(engine="DOCKER",
    // image="centos"))
    @Binary(binary = "ls", workingDir = "${APP_DIR}")
    void ls(@Parameter(type = Type.FILE, direction = Direction.OUT, stream = StdIOStream.STDOUT) String out);

    // @Container(engine="SINGULARITY", image="/home/compss/singularity/examples/ubuntu_latest.sif",
    // binary="/test/ejecutable/exec", workingDir="/test")
    @Container(engine = "DOCKER", image = "centos", binary = "pwd", workingDir = "/test")
    void lsContainer(@Parameter(type = Type.FILE, direction = Direction.OUT, stream = StdIOStream.STDOUT) String out);

}
