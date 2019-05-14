package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.data.DataAccessId;


public class StreamAccessParams extends ObjectAccessParams {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new StreamAccessParams instance for the given object.
     * 
     * @param mode Access mode.
     * @param value Associated object.
     * @param hashCode Hashcode of the associated object.
     */
    public StreamAccessParams(AccessMode mode, DataInfoProvider dip, Object value, int hashCode) {
        super(mode, dip, value, hashCode);
    }

    @Override
    public DataAccessId register() {
        return this.dip.registerStreamAccess(this.mode, getValue(), getCode());
    }

}