package integratedtoolkit.nio;

import integratedtoolkit.nio.commands.Data;

import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.annotations.parameter.Stream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class NIOParam implements Externalizable {

    private DataType type;
    private Stream stream;
    private boolean preserveSourceData;
    private boolean writeFinalValue;

    private Object value;
    private Data data;


    public NIOParam() {
        // Only executed by externalizable
    }

    public NIOParam(DataType type, Stream stream, boolean preserveSourceData, boolean writeFinalValue, Object value, Data data) {
        this.type = type;
        this.stream = stream;
        this.value = value;
        this.preserveSourceData = preserveSourceData;
        this.writeFinalValue = writeFinalValue;
        this.data = data;
    }

    public DataType getType() {
        return this.type;
    }
    
    public Stream getStream() {
        return this.stream;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public boolean isPreserveSourceData() {
        return this.preserveSourceData;
    }

    public boolean isWriteFinalValue() {
        return this.writeFinalValue;
    }

    public Object getValue() {
        return this.value;
    }

    public void setValue(Object o) {
        this.value = o;
    }

    public Data getData() {
        return this.data;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.type = (DataType) in.readObject();
        this.stream = (Stream) in.readObject();
        this.preserveSourceData = in.readBoolean();
        this.writeFinalValue = in.readBoolean();

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
        out.writeBoolean(this.preserveSourceData);
        out.writeBoolean(this.writeFinalValue);

        out.writeObject(this.value);
        if (this.data != null) {
            out.writeObject(this.data);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[PARAM");
        sb.append("[TYPE = ").append(type).append("]");
        sb.append("[STREAM = ").append(stream).append("]");
        sb.append("[PRESERVE SOURCE DATA = ").append(preserveSourceData).append("]");
        sb.append("[WRITE FINAL VALUE = ").append(writeFinalValue).append("]");
        sb.append("[VALUE = ").append(value).append("]");
        sb.append("[DATA ").append(data).append("]");
        sb.append("]");

        return sb.toString();
    }

}
