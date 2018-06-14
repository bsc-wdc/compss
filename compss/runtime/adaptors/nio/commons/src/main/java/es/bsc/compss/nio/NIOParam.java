/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.nio;

import es.bsc.compss.nio.commands.Data;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.execution.InvocationParam;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class NIOParam implements Externalizable, InvocationParam {

    private DataType type;
    private Stream stream;
    private String prefix;
    private boolean preserveSourceData;
    private boolean writeFinalValue;

    private Object value;
    private Data data;
    private String originalName;

    public NIOParam() {
        // Only executed by externalizable
    }

    public NIOParam(DataType type, Stream stream, String prefix, boolean preserveSourceData, boolean writeFinalValue, Object value,
            Data data, String originalName) {

        this.type = type;
        this.stream = stream;
        this.prefix = prefix;
        this.value = value;
        this.preserveSourceData = preserveSourceData;
        this.writeFinalValue = writeFinalValue;
        this.data = data;
        this.originalName = originalName;
    }

    @Override
    public DataType getType() {
        return this.type;
    }

    @Override
    public Stream getStream() {
        return this.stream;
    }

    @Override
    public String getPrefix() {
        return this.prefix;
    }

    @Override
    public boolean isPreserveSourceData() {
        return this.preserveSourceData;
    }

    @Override
    public boolean isWriteFinalValue() {
        return this.writeFinalValue;
    }

    public Data getData() {
        return this.data;
    }

    @Override
    public Object getValue() {
        return this.value;
    }

    @Override
    public void setType(DataType type) {
        this.type = type;
    }

    @Override
    public void setValue(Object o) {
        this.value = o;
    }

    @Override
    public String getOriginalName() {
        return this.originalName;
    }

    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.type = (DataType) in.readObject();
        this.stream = (Stream) in.readObject();
        this.prefix = (String) in.readObject();
        this.preserveSourceData = in.readBoolean();
        this.writeFinalValue = in.readBoolean();
        this.originalName = (String) in.readObject();
        this.value = in.readObject();
        try {
            this.data = (Data) in.readObject();
        } catch (java.io.OptionalDataException e) {
            this.data = null;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.type);
        out.writeObject(this.stream);
        out.writeObject(this.prefix);
        out.writeBoolean(this.preserveSourceData);
        out.writeBoolean(this.writeFinalValue);
        out.writeObject(this.originalName);
        out.writeObject(this.value);
        if (this.data != null) {
            out.writeObject(this.data);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[PARAM");
        sb.append("[TYPE = ").append(this.type).append("]");
        sb.append("[STREAM = ").append(this.stream).append("]");
        sb.append("[PREFIX = ").append(this.prefix).append("]");
        sb.append("[PRESERVE SOURCE DATA = ").append(this.preserveSourceData).append("]");
        sb.append("[WRITE FINAL VALUE = ").append(this.writeFinalValue).append("]");
        sb.append("[ORIGINAL NAME = ").append(this.originalName).append("]");
        sb.append("[VALUE = ").append(this.value).append("]");
        sb.append("[DATA ").append(this.data).append("]");
        sb.append("]");

        return sb.toString();
    }

}
