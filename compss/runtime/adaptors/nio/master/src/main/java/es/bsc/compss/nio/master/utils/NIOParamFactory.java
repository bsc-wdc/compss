/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.nio.master.utils;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOData;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOParamCollection;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.DataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.CollectiveParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Build NIOParam from other data types.
 * 
 * @see NIOParam
 */
public class NIOParamFactory {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * Construct a NIOParam from a Parameter object. Necessary to translate master representations of parameters to
     * something transferable.
     * 
     * @param param Parameter.
     * @param node NIO Worker node
     * @return NIOParam representing this Parameter.
     */
    public static NIOParam fromParameter(Parameter param, NIOWorkerNode node, boolean fromReplicatedTask) {
        NIOParam np;
        switch (param.getType()) {
            case DIRECTORY_T:
            case FILE_T:
            case OBJECT_T:
            case PSCO_T:
            case STREAM_T:
            case EXTERNAL_STREAM_T:
            case EXTERNAL_PSCO_T:
            case BINDING_OBJECT_T:
                np = buildNioDependencyParam(param, node, fromReplicatedTask);
                break;
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                NIOParam collNioParam = buildNioDependencyParam(param, node, fromReplicatedTask);
                np = buildNioCollectionParam(param, collNioParam, node, fromReplicatedTask);
                break;
            default:
                np = buildNioBasicParam(param);
                break;
        }

        return np;
    }

    private static NIOParam buildNioDependencyParam(Parameter param, NIOWorkerNode node, boolean fromReplicatedTask) {
        DependencyParameter dPar = (DependencyParameter) param;
        Object value = dPar.getDataTarget();

        // Check if the parameter has a valid PSCO and change its type
        // OUT objects are restricted by the API
        LogicalData ld = null;
        String dataMgmtId;
        DataAccessId dAccId = dPar.getDataAccessId();
        boolean preserveSourceData = fromReplicatedTask;
        if (dAccId.isRead()) {
            ld = ((ReadingDataAccessId) dAccId).getReadDataInstance().getData();
            if (dAccId.isWrite()) {
                dataMgmtId = ((WritingDataAccessId) dAccId).getWrittenDataInstance().getRenaming();
            } else {
                dataMgmtId = ld.getName();
            }
            if (!fromReplicatedTask) {
                preserveSourceData = dPar.isSourcePreserved();
            }
        } else {
            dataMgmtId = ((WritingDataAccessId) dAccId).getWrittenDataInstance().getRenaming();
            preserveSourceData = dPar.isSourcePreserved();
        }
        if (ld != null) {
            String pscoId = ld.getPscoId();
            if (pscoId != null) {
                if (param.getType().equals(DataType.OBJECT_T)) {
                    // Change Object type if it is a PSCO
                    param.setType(DataType.PSCO_T);
                } else if (param.getType().equals(DataType.FILE_T)) {
                    // Change external object type for Python PSCO return objects
                    param.setType(DataType.EXTERNAL_PSCO_T);
                }
            }
        }

        /*
         * Fix for the is replicated tasks with inout/out parameters. We have to generate output data target according
         * to the node
         */
        if (dAccId.isWrite()) {
            if (!param.getType().equals(DataType.PSCO_T) && !param.getType().equals(DataType.EXTERNAL_PSCO_T)) {
                value = node.getOutputDataTarget(dataMgmtId, dPar);
            }
        }

        // Create the NIO Param
        boolean writeFinalValue = dAccId.isWrite(); // Only store W and RW
        NIOParam np = new NIOParam(dataMgmtId, param.getType(), param.getStream(), param.getPrefix(), param.getName(),
            param.getContentType(), param.getWeight(), param.isKeepRename(), preserveSourceData, writeFinalValue, value,
            (NIOData) dPar.getDataSource(), dPar.getOriginalName());
        return np;
    }

    private static NIOParam buildNioCollectionParam(Parameter param, NIOParam collNioParam, NIOWorkerNode node,
        boolean fromReplicatedTask) {
        if (DEBUG) {
            LOGGER.debug("Detected " + param.getType() + " parameter");
        }

        NIOParamCollection npc = new NIOParamCollection(collNioParam);

        CollectiveParameter<Parameter> collParam = (CollectiveParameter) param;
        for (Parameter subParam : collParam.getElements()) {
            npc.addElement(NIOParamFactory.fromParameter(subParam, node, fromReplicatedTask));
        }

        if (DEBUG) {
            LOGGER.debug(
                "NIOParamCollection with id = " + npc.getDataMgmtId() + " contains " + npc.getSize() + " parameters.");
        }

        return npc;
    }

    private static NIOParam buildNioBasicParam(Parameter param) {
        BasicTypeParameter btParB = (BasicTypeParameter) param;
        Object value = btParB.getValue();
        boolean preserveSourceData = false; // Basic parameters are not preserved on Worker
        boolean writeFinalValue = false; // Basic parameters are not stored on Worker
        NIOParam np = new NIOParam(null, param.getType(), param.getStream(), param.getPrefix(), param.getName(),
            param.getContentType(), param.getWeight(), param.isKeepRename(), preserveSourceData, writeFinalValue, value,
            null, DependencyParameter.NO_NAME);
        return np;
    }
}
