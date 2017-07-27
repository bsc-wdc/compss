package es.bsc.compss.nio.commands;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;


public class CommandCheckWorkerACK extends Command implements Externalizable {

    private String uuid;
    private String nodeName;


    public CommandCheckWorkerACK() {
        super();
    }

    public CommandCheckWorkerACK(String uuid, String nodeName) {
        super();
        this.uuid = uuid;
        this.nodeName = nodeName;
    }

    @Override
    public CommandType getType() {
        return CommandType.CHECK_WORKER_ACK;
    }

    @Override
    public void handle(Connection c) {
        agent.setWorkerIsReady(nodeName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uuid = (String) in.readUTF();
        nodeName = (String) in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(uuid);
        out.writeUTF(nodeName);
    }

    @Override
    public String toString() {
        return "CommandCheckWorkerACK for deployment ID " + uuid;
    }

}
