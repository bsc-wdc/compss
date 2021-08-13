package es.bsc.compss.types.resources;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class HTTPResourceDescription extends WorkerResourceDescription {

    private int connections;
    // nm: list or just a string??
    private List<String> services;


    public HTTPResourceDescription(List<String> services, int connections) {
        this.services = services;
        this.connections = connections;
    }

    public List<String> getServices() {
        return this.services;
    }

    @Override
    public boolean canHost(Implementation impl) {

        if (!impl.getTaskType().equals(TaskType.HTTP)) {
            return false;
        }

        HTTPResourceDescription hrd = (HTTPResourceDescription) impl.getRequirements();
        return this.services.contains(hrd.getServices().get(0));
    }

    @Override
    public boolean canHostDynamic(Implementation impl) {
        HTTPResourceDescription srd = (HTTPResourceDescription) impl.getRequirements();
        return srd.connections <= this.connections;
    }

    @Override
    public void mimic(ResourceDescription rd) {
        // Do nothing
    }

    @Override
    public void increase(ResourceDescription rd) {
        HTTPResourceDescription srd = (HTTPResourceDescription) rd;
        this.connections += srd.connections;
    }

    @Override
    public void increaseDynamic(ResourceDescription rd) {
        HTTPResourceDescription srd = (HTTPResourceDescription) rd;
        this.connections += srd.connections;
    }

    @Override
    public void reduce(ResourceDescription rd) {
        HTTPResourceDescription srd = (HTTPResourceDescription) rd;
        this.connections -= srd.connections;
    }

    @Override
    public ResourceDescription reduceDynamic(ResourceDescription rd) {
        HTTPResourceDescription srd = (HTTPResourceDescription) rd;
        this.connections -= srd.connections;
        return new HTTPResourceDescription(srd.services, srd.connections);
    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription constraints) {
        HTTPResourceDescription sConstraints = (HTTPResourceDescription) constraints;
        int conCommons = Math.min(sConstraints.connections, this.connections);
        return new HTTPResourceDescription(sConstraints.services, conCommons);
    }

    @Override
    public boolean isDynamicUseless() {
        return connections == 0;
    }

    @Override
    public boolean isDynamicConsuming() {
        return connections > 0;
    }

    @Override
    public String toString() {
        return "[HTTP " + "CONNECTIONS=" + this.connections + "]";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Nothing to serialize since it is never used
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // Nothing to serialize since it is never used
    }

    @Override
    public HTTPResourceDescription copy() {
        return new HTTPResourceDescription(services, connections);
    }

    @Override
    public String getDynamicDescription() {
        return "Connections:" + this.connections;
    }

    @Override
    public boolean usesCPUs() {
        return false;
    }
}
