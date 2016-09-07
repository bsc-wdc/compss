package integratedtoolkit.types;

import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public class ResourceCreationRequest {

    private final String provider;
    private final CloudMethodResourceDescription requested;
    private final int[][] requestedSimultaneousTaskCount;


    public ResourceCreationRequest(CloudMethodResourceDescription requestedResource, int[][] simultaneousTasks, String provider) {
        requested = requestedResource;
        this.provider = provider;
        requestedSimultaneousTaskCount = simultaneousTasks;
    }

    public int[][] requestedSimultaneousTaskCount() {
        return requestedSimultaneousTaskCount;
    }

    public CloudMethodResourceDescription getRequested() {
        return requested;
    }

    public String getProvider() {
        return provider;
    }

}
