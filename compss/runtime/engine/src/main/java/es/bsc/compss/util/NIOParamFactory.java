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
package es.bsc.compss.util;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOParamCollection;
import es.bsc.compss.nio.commands.NIOData;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Build NIOParam from other data types
 * @see NIOParam
 */
public class NIOParamFactory {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);

    /**
     * Construct a NIOParam from a Parameter object.
     * Necessary to translate master representations of parameters to something
     * transferable.
     * @param param Parameter
     * @return NIOParam representing this Parameter
     */
    public static NIOParam fromParameter(Parameter param) {
        DataType type = param.getType();
        NIOParam np;
        switch (type) {
            case FILE_T:
            case OBJECT_T:
            case PSCO_T:
            case EXTERNAL_PSCO_T:
            case BINDING_OBJECT_T:
            case COLLECTION_T:
                DependencyParameter dPar = (DependencyParameter) param;
                DataAccessId dAccId = dPar.getDataAccessId();
                Object value = dPar.getDataTarget();
                boolean preserveSourceData;
                if (dAccId instanceof DataAccessId.RAccessId) {
                    // Parameter is a R, has sources
                    preserveSourceData = ((DataAccessId.RAccessId) dAccId).isPreserveSourceData();
                } else if (dAccId instanceof DataAccessId.RWAccessId) {
                    // Parameter is a RW, has sources
                    preserveSourceData = ((DataAccessId.RWAccessId) dAccId).isPreserveSourceData();
                } else {
                    // Parameter is a W, it has no sources
                    preserveSourceData = false;
                }

                // Check if the parameter has a valid PSCO and change its type
                // OUT objects are restricted by the API
                String renaming = null;
                String dataMgmtId;
                DataAccessId faId = dPar.getDataAccessId();
                if (faId instanceof DataAccessId.RWAccessId) {
                    // Read write mode
                    DataAccessId.RWAccessId rwaId = (DataAccessId.RWAccessId) faId;
                    renaming = rwaId.getReadDataInstance().getRenaming();
                    dataMgmtId = rwaId.getWrittenDataInstance().getRenaming();
                } else if (faId instanceof DataAccessId.RAccessId) {
                    // Read only mode
                    DataAccessId.RAccessId raId = (DataAccessId.RAccessId) faId;
                    renaming = raId.getReadDataInstance().getRenaming();
                    dataMgmtId = renaming;
                } else {
                    DataAccessId.WAccessId waId = (DataAccessId.WAccessId) faId;
                    dataMgmtId = waId.getWrittenDataInstance().getRenaming();
                }
                if (renaming != null) {
                    String pscoId = Comm.getData(renaming).getPscoId();
                    if (pscoId != null) {
                        if (type.equals(DataType.OBJECT_T)) {
                            // Change Object type if it is a PSCO
                            param.setType(DataType.PSCO_T);
                        } else if (type.equals(DataType.FILE_T)) {
                            // Change external object type (Workaround for Python PSCO return objects)
                            param.setType(DataType.EXTERNAL_PSCO_T);
                        }
                        type = param.getType();
                    }
                }

                // Create the NIO Param
                boolean writeFinalValue = !(dAccId instanceof DataAccessId.RAccessId); // Only store W and RW
                np = new NIOParam(dataMgmtId, type, param.getStream(), param.getPrefix(), param.getName(), preserveSourceData, writeFinalValue, value,
                        (NIOData) dPar.getDataSource(), dPar.getOriginalName());
                break;

            default:
                BasicTypeParameter btParB = (BasicTypeParameter) param;
                value = btParB.getValue();
                preserveSourceData = false; // Basic parameters are not preserved on Worker
                writeFinalValue = false; // Basic parameters are not stored on Worker
                np = new NIOParam(null, type, param.getStream(), param.getPrefix(), param.getName(), preserveSourceData, writeFinalValue, value, null,
                        DependencyParameter.NO_NAME);
                break;
        }

        if(type == DataType.COLLECTION_T) {
            LOGGER.debug("COLLECTION_T detected");
            NIOParamCollection ret = new NIOParamCollection(
                    np.getDataMgmtId(),
                    np.getType(),
                    np.getStream(),
                    np.getPrefix(),
                    np.getName(),
                    np.isPreserveSourceData(),
                    np.isWriteFinalValue(),
                    np.getValue(),
                    np.getData(),
                    np.getOriginalName()
            );
            CollectionParameter cp = (CollectionParameter)param;
            for(Parameter subParam : cp.getParameters()) {
                LOGGER.debug("Adding " + subParam);
                ret.getCollectionParameters().add(NIOParamFactory.fromParameter(subParam));
            }
            LOGGER.debug("NIOParamCollection contains " + ret.getCollectionParameters().size() + " parameters.");
            np = ret;
        }
        return np;
    }
}
