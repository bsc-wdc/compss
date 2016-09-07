package integratedtoolkit.nio.commands;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;


public class CommandTracingID extends Command implements Externalizable {

    private int id;
    private int tag;


    public CommandTracingID() {
        super();
    }

    public CommandTracingID(int id, int tag) {
        super();
        this.id = id;
        this.tag = tag;
    }

    @Override
    public CommandType getType() {
        return CommandType.DATA_DEMAND;
    }

    @Override
    public void handle(Connection c) {
        agent.addConnectionAndPartner(c, id, tag);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();
        tag = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(tag);
    }

    @Override
    public String toString() {
        return "Request with sender ID: " + id;
    }

}
