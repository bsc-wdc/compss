package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.data.DataAccessId;


public class ObjectAccessParams extends AccessParams {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private int hashCode;
    private Object value;


    /**
     * Creates a new ObjectAccessParams instance for the given object.
     * 
     * @param mode Access mode.
     * @param value Associated object.
     * @param hashCode Hashcode of the associated object.
     */
    public ObjectAccessParams(AccessMode mode, DataInfoProvider dip, Object value, int hashCode) {
        super(mode, dip);
        this.value = value;
        this.hashCode = hashCode;
    }

    /**
     * Returns the associated object.
     * 
     * @return The associated object.
     */
    public Object getValue() {
        return value;
    }

    /**
     * Returns the hashcode of the associated object.
     * 
     * @return The hashcode of the associated object.
     */
    public int getCode() {
        return hashCode;
    }

    @Override
    public DataAccessId register() {
        return this.dip.registerObjectAccess(this.mode, this.value, this.hashCode);
    }
}