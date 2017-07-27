package es.bsc.compss.nio.commands;

import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.nio.NIOURI;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;


public class Data implements Externalizable {

    // Name of the data to send
    private String name;

    // Sources list
    private LinkedList<NIOURI> sources;


    public Data() {
    }

    public Data(String name, NIOURI uri) {
        this.name = name;
        this.sources = new LinkedList<NIOURI>();
        this.sources.add(uri);
    }

    public Data(LogicalData ld) {
        this.name = ld.getName();
        this.sources = new LinkedList<NIOURI>();
        for (MultiURI uri : ld.getURIs()) {
            try {
                Object o = uri.getInternalURI(NIOAgent.ID);
                if (o != null) {
                    this.sources.add((NIOURI) o);
                }
            } catch (UnstartedNodeException une) {
                // Ignore internal URI.
            }
        }
    }

    // Returns true if the name of the data is the same
    // Returns false otherwise
    public boolean compareTo(Data n) {
        if (n.name.compareTo(name) == 0) {
            return true;
        } else {
            return false;
        }
    }

    public String getName() {
        return name;
    }

    public LinkedList<NIOURI> getSources() {
        return sources;
    }

    public NIOURI getFirstURI() {
        if (sources != null && !sources.isEmpty()) {
            return sources.getFirst();
        }
        return null;
    }

    // Returns a URI inside the same host or null if the data is not available
    public NIOURI getURIinHost(String hostname) {
        for (NIOURI loc : sources) {
            String hostAndPort = loc.getHost().toString();
            String host = hostAndPort.substring(0, hostAndPort.indexOf(":"));
            if (host.equals(hostname)) {
                return loc;
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
        sources = (LinkedList<NIOURI>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(sources);
    }

    @Override
    public String toString() {
        return name + "@" + sources;
    }
}
