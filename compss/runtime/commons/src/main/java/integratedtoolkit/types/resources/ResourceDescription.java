package integratedtoolkit.types.resources;

import integratedtoolkit.types.Implementation;


public abstract class ResourceDescription {

    public ResourceDescription() {
    }

    public ResourceDescription(ResourceDescription rd) {
    }

    public abstract boolean canHost(Implementation<?> impl);

}
