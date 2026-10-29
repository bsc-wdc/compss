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
package es.bsc.compss.executor.external;

import es.bsc.compss.execution.types.ExecutorContext;

/**
 * Creates the mirror of an executor context. Every mirror exposes its factory as a constant so that the platform can
 * create the mirror on the first lazy handle lookup; implementations are anonymous classes around the mirror
 * constructor, since this codebase does not use functional interfaces.
 *
 * @param <M> mirror type created by this factory
 */
public interface MirrorFactory<M extends ExecutorContextMirror<?>> {

    /**
     * Creates the mirror of the given platform.
     *
     * @param platform platform whose mirror must be created
     * @return newly created mirror of the platform
     */
    M create(ExecutorContext platform);

}
