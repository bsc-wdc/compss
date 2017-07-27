package es.bsc.compss.nio;

import es.bsc.compss.nio.commands.Data;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Stream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class NIOParam implements Externalizable {

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

    public DataType getType() {
        return this.type;
    }

    public Stream getStream() {
        return this.stream;
    }

    public String getPrefix() {
        return this.prefix;
    }

    public boolean isPreserveSourceData() {
        return this.preserveSourceData;
    }

    public boolean isWriteFinalValue() {
        return this.writeFinalValue;
    }

    public Data getData() {
        return this.data;
    }

    public Object getValue() {
        return this.value;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public void setValue(Object o) {
        this.value = o;
    }

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
