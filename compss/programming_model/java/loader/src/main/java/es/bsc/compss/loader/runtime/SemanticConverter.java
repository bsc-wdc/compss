/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.loader.runtime;

import es.bsc.compss.loader.runtime.data.access.AccessMode;
import es.bsc.compss.loader.runtime.task.FailurePolicy;
import es.bsc.compss.loader.runtime.task.parameter.StdIOStream;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;

public class SemanticConverter {

    /**
     * Creates an Access Mode from the Direction of a parameter.
     *
     * @param dir direction of the parameter
     * @return mode that the task will access the value of the parameter
     */
    public static AccessMode getAccessMode(Direction dir) {
        switch (dir) {
            case IN:
                return AccessMode.READ;
            case OUT:
                return AccessMode.GENERATE;
            case INOUT:
                return AccessMode.UPDATE;
            case CONCURRENT:
                return AccessMode.CONCURRENT_UPDATE;
            case COMMUTATIVE:
                return AccessMode.COMMUTATIVE_UPDATE;
            case IN_DELETE:
                return AccessMode.READ_AND_DELETE;
            default:
                throw new IllegalArgumentException("Unknown Direction " + dir);
        }
    }

    /**
     * Creates a stdIOStream from the stdIOStream of a parameter annotation.
     *
     * @param stream annotated stdIOStream
     * @return stdIOStreams known by the runtime system
     */
    public static StdIOStream getStdIOStream(es.bsc.compss.types.annotations.parameter.StdIOStream stream) {
        switch (stream) {
            case STDIN:
                return StdIOStream.STDIN;
            case STDOUT:
                return StdIOStream.STDOUT;
            case STDERR:
                return StdIOStream.STDERR;
            case UNSPECIFIED:
                return StdIOStream.UNSPECIFIED;
            default:
                throw new IllegalArgumentException("Unknown Stream " + stream);
        }
    }

    /**
     * Creates a FailurePolicy from the OnFailure of a task annotation.
     *
     * @param onFailure annotated onFailure value
     * @return corresponding FailurePolicy
     */
    public static FailurePolicy getFailurePolicy(OnFailure onFailure) {
        switch (onFailure) {
            case FAIL:
                return FailurePolicy.FAIL;
            case RETRY:
                return FailurePolicy.RETRY;
            case IGNORE:
                return FailurePolicy.IGNORE;
            case CANCEL_SUCCESSORS:
                return FailurePolicy.CANCEL_SUCCESSORS;
            default:
                throw new IllegalArgumentException("Unknown OnFailure " + onFailure);
        }
    }
}
