package integratedtoolkit.nio;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.nio.commands.Data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class NIOParam implements Externalizable {

    private DataType type;
    private boolean preserveSourceData;
    private boolean writeFinalValue;

    private Object value;
    private Data data;


    public NIOParam() {
        // Only executed by externalizable
    }

    public NIOParam(DataType type, boolean preserveSourceData, boolean writeFinalValue, Object value, Data data) {
        this.type = type;
        this.value = value;
        this.preserveSourceData = preserveSourceData;
        this.writeFinalValue = writeFinalValue;
        this.data = data;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public boolean isPreserveSourceData() {
        return preserveSourceData;
    }

    public boolean isWriteFinalValue() {
        return writeFinalValue;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object o) {
        value = o;
    }

    public Data getData() {
        return data;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = (DataType) in.readObject();
        preserveSourceData = in.readBoolean();
        writeFinalValue = in.readBoolean();

        value = in.readObject();
        try {
            data = (Data) in.readObject();
        } catch (java.io.OptionalDataException e) {
            data = null;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(type);
        out.writeBoolean(preserveSourceData);
        out.writeBoolean(writeFinalValue);

        out.writeObject(value);
        if (data != null) {
            out.writeObject(data);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[PARAM");
        sb.append("[TYPE = ").append(type).append("]");
        sb.append("[PRESERVE SOURCE DATA = ").append(preserveSourceData).append("]");
        sb.append("[WRITE FINAL VALUE = ").append(writeFinalValue).append("]");
        sb.append("[VALUE = ").append(value).append("]");
        sb.append("[DATA ").append(data).append("]");
        sb.append("]");

        return sb.toString();
    }

}
