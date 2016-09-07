package integratedtoolkit.nio.commands;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.nio.NIOAgent;
import es.bsc.comm.Connection;


public class CommandDataNegate extends Command implements Externalizable {

    private Data d;

    // Whether the node has the file or not
    private boolean hosted;


    public CommandDataNegate(NIOAgent ng, Data d, boolean hosted) {
        super(ng);
        this.d = d;
        this.hosted = hosted;
    }

    @Override
    public CommandType getType() {
        return CommandType.DATA_NEGATE;
    }

    @Override
    public void handle(Connection c) {
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        d = (Data) in.readObject();
        hosted = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(d);
        out.writeBoolean(hosted);
    }

    @Override
    public String toString() {
        return "Data " + d + " can't be send" + (hosted ? ", although it is in the node" : " since the node does not have it");
    }

}
