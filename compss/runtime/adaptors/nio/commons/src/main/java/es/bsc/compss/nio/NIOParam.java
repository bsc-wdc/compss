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

import es.bsc.compss.nio.commands.NIOData;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.execution.InvocationParam;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class NIOParam implements Externalizable, InvocationParam {

    private String dataMgmtId;
    private DataType type;
    private Stream stream;
    private String prefix;
    private boolean preserveSourceData;
    private boolean writeFinalValue;

    private Object value;
    private NIOData source;
    private String originalName;

    private Class<?> valueClass;

    public NIOParam() {
        // Only executed by externalizable
    }

    public NIOParam(String dataMgmtId, DataType type, Stream stream, String prefix, boolean preserveSourceData, boolean writeFinalValue, Object value,
            NIOData data, String originalName) {
        this.dataMgmtId = dataMgmtId;
        this.type = type;
        this.stream = stream;
        this.prefix = prefix;
        this.value = value;
        this.preserveSourceData = preserveSourceData;
        this.writeFinalValue = writeFinalValue;
        this.source = data;
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

    public NIOData getData() {
        return this.source;
    }

    @Override
    public String getDataMgmtId() {
        return this.dataMgmtId;
    }

    @Override
    public String getSourceDataId() {
        return source.getDataMgmtId();
    }

    @Override
    public List<NIOURI> getSources() {
        return source.getSources();
    }

    @Override
    public String getOriginalName() {
        return this.originalName;
    }

    @Override
    public void setOriginalName(String originalName) {
        this.originalName = originalName;
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
    public void setValueClass(Class<?> valueClass) {
        this.valueClass = valueClass;
    }

    @Override
    public Class<?> getValueClass() {
        return valueClass;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.dataMgmtId = (String) in.readObject();
        this.type = (DataType) in.readObject();
        this.stream = (Stream) in.readObject();
        this.prefix = (String) in.readObject();
        this.preserveSourceData = in.readBoolean();
        this.writeFinalValue = in.readBoolean();
        this.originalName = (String) in.readObject();
        this.value = in.readObject();
        try {
            this.source = (NIOData) in.readObject();
        } catch (java.io.OptionalDataException e) {
            this.source = null;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.dataMgmtId);
        out.writeObject(this.type);
        out.writeObject(this.stream);
        out.writeObject(this.prefix);
        out.writeBoolean(this.preserveSourceData);
        out.writeBoolean(this.writeFinalValue);
        out.writeObject(this.originalName);
        out.writeObject(this.value);
        if (this.source != null) {
            out.writeObject(this.source);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[PARAM");
        sb.append("[MGMT ID = ").append(this.dataMgmtId).append("]");
        sb.append("[TYPE = ").append(this.type).append("]");
        sb.append("[STREAM = ").append(this.stream).append("]");
        sb.append("[PREFIX = ").append(this.prefix).append("]");
        sb.append("[PRESERVE SOURCE DATA = ").append(this.preserveSourceData).append("]");
        sb.append("[WRITE FINAL VALUE = ").append(this.writeFinalValue).append("]");
        sb.append("[ORIGINAL NAME = ").append(this.originalName).append("]");
        sb.append("[VALUE = ").append(this.value).append("]");
        sb.append("[DATA ").append(this.source).append("]");
        sb.append("]");

        return sb.toString();
    }

}
