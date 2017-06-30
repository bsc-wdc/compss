package integratedtoolkit.types;

import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;

public class ResourceCreationRequest {

    private final CloudProvider provider;
    private final CloudMethodResourceDescription requested;
    private int[][] requestedSimultaneousTaskCount;

    public ResourceCreationRequest(CloudMethodResourceDescription requestedResource, int[][] simultaneousTasks, CloudProvider cp) {
        requested = requestedResource;
        this.provider = cp;
        requestedSimultaneousTaskCount = simultaneousTasks;
    }

    public int[][] requestedSimultaneousTaskCount() {
        return requestedSimultaneousTaskCount;
    }

    public void updateRequestedSimultaneousTaskCount(int[][] newRequestedSimultaneousTaskCount) {
        requestedSimultaneousTaskCount = newRequestedSimultaneousTaskCount;
    }

    public CloudMethodResourceDescription getRequested() {
        return requested;
    }

    public CloudProvider getProvider() {
        return provider;
    }

}
