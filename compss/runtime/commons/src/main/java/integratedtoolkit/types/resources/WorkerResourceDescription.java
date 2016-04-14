package integratedtoolkit.types.resources;

import org.w3c.dom.Node;


public abstract class WorkerResourceDescription extends ResourceDescription {

    public WorkerResourceDescription() {
        super();
    }

    public WorkerResourceDescription(Node n) {
    }

    public WorkerResourceDescription(ResourceDescription desc) {
        super(desc);
    }
}
