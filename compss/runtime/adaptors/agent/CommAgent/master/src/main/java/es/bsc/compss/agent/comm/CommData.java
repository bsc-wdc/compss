package es.bsc.compss.agent.comm;

import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.nio.NIOData;

import java.util.LinkedList;
import java.util.List;


public class CommData extends NIOData {

    private final List<RemoteDataLocation> locations;


    /**
     * Creates a new CommData instance.
     *
     * @param name Data name.
     */
    public CommData(String name) {
        super(name);
        locations = new LinkedList<>();
    }

    /**
     * Adds a new Remote data location to the data.
     * 
     * @param rdl location to be added to the data
     */
    public void addRemoteLocation(RemoteDataLocation rdl) {
        locations.add(rdl);
    }

    /**
     * Returns a list with all the remote location of the data.
     * 
     * @return list of all the remote locations of the data.
     */
    public List<RemoteDataLocation> getRemoteLocations() {
        return locations;
    }
}
