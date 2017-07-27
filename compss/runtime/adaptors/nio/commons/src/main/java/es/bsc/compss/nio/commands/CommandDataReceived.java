package es.bsc.compss.nio.commands;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;
import es.bsc.compss.nio.NIOAgent;


public class CommandDataReceived extends Command implements Externalizable {

    private int transfergroupID;


    public CommandDataReceived() {
    }

    public CommandDataReceived(NIOAgent ng, int transfergroupID) {
        super(ng);
        this.transfergroupID = transfergroupID;
    }

    @Override
    public CommandType getType() {
        return CommandType.DATA_RECEIVED;
    }

    @Override
    public void handle(Connection c) {
        c.finishConnection();
        NIOAgent nm = (NIOAgent) agent;
        nm.copiedData(transfergroupID);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        transfergroupID = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(transfergroupID);
    }

    @Override
    public String toString() {
        return "Data for transfer group" + transfergroupID + "has been received in the remote worker";
    }

}
