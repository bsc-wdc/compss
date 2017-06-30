package integratedtoolkit.connectors.conn.util;

import es.bsc.conn.Connector;
import es.bsc.conn.exceptions.ConnException;
import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.VirtualResource;
import integratedtoolkit.connectors.ConnectorException;
import java.util.Map;


public class ConnectorProxy {

    // Constraints default values
    private static final String ERROR_NO_CONN = "ERROR: Connector specific implementation is null";

    private final Connector connector;


    public ConnectorProxy(Connector conn) throws ConnectorException {
        if (conn == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
        this.connector = conn;
    }

    public Object create(HardwareDescription hardwareDescription, SoftwareDescription softwareDescription, Map<String, String> properties)
            throws ConnectorException {

        if (connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
        Object created;
        try {
            created = connector.create(hardwareDescription, softwareDescription, properties);
        } catch (ConnException ce) {
            throw new ConnectorException(ce);
        }
        return created;
    }

    public void destroy(Object id) throws ConnectorException {
        if (connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }

        connector.destroy(id);
    }

    public VirtualResource waitUntilCreation(Object id) throws ConnectorException {
        if (connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }

        VirtualResource vr;
        try {
            vr = connector.waitUntilCreation(id);
        } catch (ConnException ce) {
            throw new ConnectorException(ce);
        }
        return vr;
    }

    public float getPriceSlot(VirtualResource vr, float defaultPrice) {
        if (connector == null) {
            return defaultPrice;
        }

        return connector.getPriceSlot(vr);
    }

    public long getTimeSlot(long defaultLength) {
        if (connector == null) {
            return defaultLength;
        }
        return connector.getTimeSlot();
    }

    public void close() {
        connector.close();
    }

}
