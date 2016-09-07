package integratedtoolkit.nio.commands;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.nio.NIOAgent;
import es.bsc.comm.Connection;


public class CommandDataDemand extends Command implements Externalizable {

    private Data d;
    private int id;


    public CommandDataDemand() {
        super();
    }

    public CommandDataDemand(NIOAgent ng, Data d, int receiverID) {
        super(ng);
        this.d = d;
        this.id = receiverID;
    }

    @Override
    public CommandType getType() {
        return CommandType.DATA_DEMAND;
    }

    @Override
    public void handle(Connection c) {
        boolean slot = agent.tryAcquireSendSlot(c);
        if (!slot) {// There are no slots available
            // TODO: ENABLE DATA NEGATE COMMANDS
            agent.sendData(c, d, id);
            // agent.sendDataNegate(c, d, true);
        } else { // There is a slot and the data exists
            agent.sendData(c, d, id);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        d = (Data) in.readObject();
        id = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(d);
        out.writeInt(id);
    }

    @Override
    public String toString() {
        return "Request for sending data " + d;
    }

}
