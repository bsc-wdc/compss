/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.connectors.conn.util;

import es.bsc.conn.Connector;
import es.bsc.conn.exceptions.ConnException;
import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.VirtualResource;
import es.bsc.compss.connectors.ConnectorException;

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

    public Object create(String name, HardwareDescription hardwareDescription, SoftwareDescription softwareDescription, Map<String, String> properties)
            throws ConnectorException {

        if (connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
        Object created;
        try {
            created = connector.create(name, hardwareDescription, softwareDescription, properties);
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
