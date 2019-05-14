package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.location.DataLocation;


public class FileAccessParams extends AccessParams {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private DataLocation loc;


    /**
     * Creates a new FileAccessParams instance with the given mode {@code mode} and for the given file location
     * {@code loc}.
     * 
     * @param mode Access mode.
     * @param loc File location.
     */
    public FileAccessParams(AccessMode mode, DataInfoProvider dip, DataLocation loc) {
        super(mode, dip);
        this.loc = loc;
    }

    /**
     * Returns the file location.
     * 
     * @return The file location.
     */
    public DataLocation getLocation() {
        return this.loc;
    }

    @Override
    public DataAccessId register() {
        return this.dip.registerFileAccess(this.mode, this.loc);
    }

}
