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
package es.bsc.compss.nio;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.execution.InvocationParam;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


/**
 * Representation of a data parameter for the NIO Adaptor.
 */
public class NIOParam implements Externalizable, InvocationParam {

    private String dataMgmtId;
    private DataType type;
    private StdIOStream stream;
    private String prefix;
    private String name;
    private String contentType;
    private double weight;
    private boolean keepRename;

    private boolean preserveSourceData;
    private boolean writeFinalValue;

    private Object value;
    private NIOData source;
    private String originalName;
    private String renamedName;

    private String targetPath;

    private Class<?> valueClass;


    /**
     * Creates a new NIOParam instance for externalization.
     */
    public NIOParam() {
        // Only executed by externalizable
    }

    /**
     * Creates a new NIOParam instance with the given information.
     *
     * @param dataMgmtId Renaming Id.
     * @param type Data Type.
     * @param stream Std IO stream type.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param preserveSourceData Whether to keep the source data or not.
     * @param writeFinalValue Whether to write the output value or not.
     * @param value The original value.
     * @param data The original data.
     * @param originalName The original parameter name.
     */
    public NIOParam(String dataMgmtId, DataType type, StdIOStream stream, String prefix, String name,
        String contentType, double weight, boolean keepRename, boolean preserveSourceData, boolean writeFinalValue,
        Object value, NIOData data, String originalName) {

        this.dataMgmtId = dataMgmtId;
        this.type = type;
        this.stream = stream;
        this.prefix = prefix;
        this.name = name;
        if (contentType == null) {
            this.contentType = "null";
        } else {
            this.contentType = contentType;
        }
        this.weight = weight;
        this.keepRename = keepRename;
        this.value = value;
        this.preserveSourceData = preserveSourceData;
        this.writeFinalValue = writeFinalValue;
        this.source = data;
        this.originalName = originalName;
    }

    /**
     * Creates a new NIOParam instance copying the given NIOParam internal fields.
     *
     * @param p NIOParam to copy.
     */
    public NIOParam(NIOParam p) {
        this.dataMgmtId = p.dataMgmtId;
        this.type = p.type;
        this.stream = p.stream;
        this.prefix = p.prefix;
        this.name = p.name;
        this.contentType = p.contentType;
        this.weight = p.weight;
        this.keepRename = p.keepRename;
        this.value = p.value;
        this.preserveSourceData = p.preserveSourceData;
        this.writeFinalValue = p.writeFinalValue;
        this.source = p.source;
        this.originalName = p.originalName;
    }

    @Override
    public DataType getType() {
        return this.type;
    }

    @Override
    public boolean isCollective() {
        return false;
    }

    @Override
    public StdIOStream getStdIOStream() {
        return this.stream;
    }

    @Override
    public String getPrefix() {
        return this.prefix;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getContentType() {
        return this.contentType;
    }

    @Override
    public double getWeight() {
        return this.weight;
    }

    @Override
    public boolean isKeepRename() {
        return this.keepRename;
    }

    @Override
    public boolean isPreserveSourceData() {
        return this.preserveSourceData;
    }

    @Override
    public boolean isWriteFinalValue() {
        return this.writeFinalValue;
    }

    @Override
    public String getDataMgmtId() {
        return this.dataMgmtId;
    }

    @Override
    public String getSourceDataId() {
        return source != null ? source.getDataMgmtId() : null;
    }

    @Override
    public List<NIOUri> getSources() {
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
    public String getRenamedName() {
        return this.renamedName;
    }

    @Override
    public void setRenamedName(String renamedName) {
        this.renamedName = renamedName;
    }

    @Override
    public Object getValue() {
        return this.value;
    }

    @Override
    public Class<?> getValueClass() {
        return this.valueClass;
    }

    /**
     * Returns the source data.
     *
     * @return The source data.
     */
    public NIOData getData() {
        return this.source;
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

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    @Override
    public void resultIsForwarded() {
        // Do nothing
    }

    @Override
    public boolean isForwardedResult() {
        return false;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.dataMgmtId = (String) in.readObject();
        this.type = (DataType) in.readObject();
        this.stream = (StdIOStream) in.readObject();
        this.prefix = in.readUTF();
        this.name = in.readUTF();
        this.contentType = in.readUTF();
        this.weight = in.readDouble();
        this.keepRename = in.readBoolean();
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
        out.writeUTF(this.prefix);
        out.writeUTF(this.name);
        out.writeUTF(this.contentType);
        out.writeDouble(this.weight);
        out.writeBoolean(this.keepRename);
        out.writeBoolean(this.preserveSourceData);
        out.writeBoolean(this.writeFinalValue);
        out.writeObject(this.originalName);
        out.writeObject(this.value);
        if (this.source != null) {
            out.writeObject(this.source);
        }
    }

    /**
     * Dumps the internal information into the given StringBuilder.
     *
     * @param sb StringBuilder where to dump the internal information.
     */
    protected void dumpInternalInfo(StringBuilder sb) {
        sb.append("\"mgmt_id\":\"").append(this.dataMgmtId).append("\",");
        sb.append("\"type\":\"").append(this.type).append("\",");
        sb.append("\"io_stream\":\"").append(this.stream).append("\",");
        sb.append("\"prefix\":\"").append(this.prefix).append("\",");
        sb.append("\"name\":\"").append(this.name).append("\",");
        sb.append("\"content_type\":\"").append(this.contentType).append("\",");
        sb.append("\"keep_rename\":").append(this.keepRename).append(",");
        sb.append("\"preserve_source_data\":").append(this.preserveSourceData).append(",");
        sb.append("\"write_final_value\":").append(this.writeFinalValue).append(",");
        sb.append("\"original_name\":\"").append(this.originalName).append("\",");
        sb.append("\"value\":");
        switch (this.type) {
            case STRING_T:
            case STRING_64_T:
            case FILE_T:
            case OBJECT_T:
            case PSCO_T:
            case WSTRING_T:
            case BINDING_OBJECT_T:
            case DICT_COLLECTION_T:
                sb.append("\"").append(this.value).append("\"");
                break;
            default:
                sb.append(this.value);
        }
        sb.append(",");
        sb.append("\"data\":").append(this.source).append(",");
        sb.append("\"stored_path\":").append(this.targetPath);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        dumpInternalInfo(sb);
        sb.append("}");
        return sb.toString();
    }

}
