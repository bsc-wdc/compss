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

package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.ImplementationDefinition;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class FakeImplDefinition implements ImplementationDefinition {

    private final String signature;


    public FakeImplDefinition(String signature) {
        this.signature = signature;
    }

    @Override
    public TaskType getTaskType() {
        return null;
    }

    @Override
    public String toJSON() {
        return "{\"signature\":\"" + this.signature + "\"}";
    }

    @Override
    public String toShortFormat() {
        return "Fake " + this.signature;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

    }
}
