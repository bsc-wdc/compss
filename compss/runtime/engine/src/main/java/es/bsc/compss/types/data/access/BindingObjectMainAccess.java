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
package es.bsc.compss.types.data.access;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessparams.BindingObjectAccessParams;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.BindingObjectTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.data.params.BindingObjectData;
import es.bsc.compss.util.ErrorManager;
import java.util.concurrent.Semaphore;


/**
 * Handling of an access from the main code to a binding object.
 */
public class BindingObjectMainAccess
    extends ObjectMainAccess<BindingObject, BindingObjectData, BindingObjectAccessParams> {

    /**
     * Creates a new BindingObjectMainAccess instance.
     *
     * @param app Id of the application accessing the BindingObject.
     * @param dir operation performed.
     * @param bo Associated BindingObject.
     * @param hashCode Hashcode of the associated BindingObject.
     * @return new BindingObjectAccessParams instance
     */
    public static final BindingObjectMainAccess constructBOMA(Application app, Direction dir, BindingObject bo,
        int hashCode) {
        BindingObjectAccessParams boap = BindingObjectAccessParams.constructBOAP(app, dir, bo, hashCode);
        return new BindingObjectMainAccess(app, boap);
    }

    protected BindingObjectMainAccess(Application app, BindingObjectAccessParams params) {
        super(app, params);
    }

    @Override
    public boolean resultRemainOnMain() {
        return false;
    }

    /**
     * Fetches the last version of the object.
     *
     * @param daId Data Access Id.
     * @return Location of the transferred open file.
     */
    @Override
    public BindingObject fetch(EngineDataAccessId daId) {
        LOGGER_API.debug("[AccessProcessor] Obtaining " + this.getParameters().getDataDescription());

        // Get target information
        ReadingDataAccessId raId = (ReadingDataAccessId) daId;
        EngineDataInstanceId diId = raId.getReadDataInstance();
        String targetName = diId.getRenaming();

        if (API_DEBUG) {
            LOGGER_API.debug("[DataInfoProvider] Requesting getting object " + targetName);
        }
        LogicalData srcData = diId.getData();
        if (API_DEBUG) {
            LOGGER_API.debug("[DataInfoProvider] Logical data for binding object is:" + srcData);
        }
        if (srcData == null) {
            ErrorManager.error("Unregistered data " + targetName);
            return null;
        }
        if (API_DEBUG) {
            LOGGER_API.debug("Requesting tranfers binding object " + targetName + " to " + Comm.getAppHost().getName());
        }

        BindingObject srcBO = BindingObject.generate(srcData.getURIs().get(0).getPath());
        BindingObject tgtBO = new BindingObject(targetName, srcBO.getType(), srcBO.getElements());
        LogicalData tgtLd = srcData;
        DataLocation targetLocation = new BindingObjectLocation(Comm.getAppHost(), tgtBO);
        BindingObjectTransferable transfer = new BindingObjectTransferable();
        Semaphore sem = new Semaphore(0);
        Comm.getAppHost().getData(srcData, targetLocation, tgtLd, transfer, new OneOpWithSemListener(sem));
        if (API_DEBUG) {
            LOGGER_API.debug(" Setting tgtName " + transfer.getDataTarget() + " in " + Comm.getAppHost().getName());
        }
        sem.acquireUninterruptibly();

        String boStr = transfer.getDataTarget();
        BindingObject bo = BindingObject.generate(boStr);
        return bo;
    }

    @Override
    public boolean isAccessFinishedOnRegistration() {
        return true;
    }
}
