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
package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.implementations.TaskType;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

public abstract class NativeDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    private Lang lang;


    /**
     * Creates a new MethodImplementation for serialization.
     */
    public NativeDefinition() {
        // For externalizable
    }

    public NativeDefinition(Lang lang) {
        this.lang = lang;
    }

    /**
     * Creates a new Definition from string array.
     *
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public NativeDefinition(String[] implTypeArgs, int offset) {
        this.lang = Lang.valueOf(implTypeArgs[offset]);
        if (lang == Lang.UNKNOWN) {
            throw new IllegalArgumentException("Invalid language for native method.");
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(this.lang.name());
    }

    /**
     * Returns language of the implementation.
     *
     * @return language of the implementation
     */
    public Lang getLang() {
        return lang;
    }

    @Override
    public String toJSON() {
        return "\"lang\":\"" + this.lang + "\"";
    }

    @Override
    public String toShortFormat() {
        return this.lang.name();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.lang = Lang.values()[in.readInt()];
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.lang.ordinal());
    }

    @Override
    public String toString() {
        return "\t Lang: " + this.lang + "\n";
    }

    @Override
    public final TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
