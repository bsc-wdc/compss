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
package es.bsc.compss.agent;

import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.ApplicationParameterCollection;
import es.bsc.compss.agent.types.PrivateRemoteDataLocation;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.agent.types.SharedRemoteDataLocation;
import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.ParameterCollectionMonitor;
import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.SharedDisk;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.worker.COMPSsException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class AppMonitor implements ApplicationRunner {

    private long appId;
    private TaskResult[] taskResults;
    private COMPSsException exception;

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);


    /**
     * Constructs a new Application monitor.
     *
     * @param args Monitored execution's arguments
     * @param target Monitored execution's target
     * @param results Monitored execution's results
     */
    public AppMonitor(ApplicationParameter[] args, ApplicationParameter target, ApplicationParameter[] results) {
        int argsCount = args.length;
        int resultsCount = results.length;
        int numParams = argsCount + resultsCount + (target != null ? 1 : 0);
        this.taskResults = new TaskResult[numParams];

        int offset = 0;
        for (; offset < argsCount; offset++) {
            ApplicationParameter param = args[offset];
            this.taskResults[offset] = buildResult(param);
        }

        if (target != null) {
            this.taskResults[offset] = buildResult(target);
            offset++;
        }

        for (int resultIdx = 0; resultIdx < results.length; resultIdx++) {
            ApplicationParameter param = results[resultIdx];
            this.taskResults[offset] = buildResult(param);
            offset++;
        }
    }

    private TaskResult buildResult(ApplicationParameter param) {
        switch (param.getType()) {
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                ApplicationParameterCollection<ApplicationParameter> colParam;
                colParam = (ApplicationParameterCollection<ApplicationParameter>) param;
                List<ApplicationParameter> subParams = colParam.getCollectionParameters();
                int numElements = subParams.size();
                TaskResult[] subResults = new TaskResult[numElements];
                int subElementIdx = 0;
                for (ApplicationParameter subParam : subParams) {
                    subResults[subElementIdx] = buildResult(subParam);
                    subElementIdx++;
                }
                return new CollectionTaskResult(param.getDataMgmtId(), subResults, this);
            default:
                return new TaskResult(param.getDataMgmtId(), this);
        }
    }

    public void setAppId(long appId) {
        this.appId = appId;
    }

    public long getAppId() {
        return this.appId;
    }

    public COMPSsException getException() {
        return exception;
    }

    public TaskResult[] getResults() {
        return AppMonitor.this.taskResults;
    }

    public void setResults(TaskResult[] params) {
        AppMonitor.this.taskResults = params;
    }

    @Override
    public void stalledApplication() {
        // No need to do anything
    }

    @Override
    public void readyToContinue(Semaphore sem) {
        // No need to do anything
    }

    @Override
    public final void onCancellation() {
        specificOnCancellation();
    }

    protected abstract void specificOnCancellation();

    @Override
    public final void onException(COMPSsException e) {
        AppMonitor.this.exception = e;
        specificOnException(e);
    }

    protected abstract void specificOnException(COMPSsException e);

    @Override
    public final void onCompletion() {
        new Thread() {

            @Override
            public void run() {
                Agent.finishedApplication(appId);
                specificOnCompletion();
            }
        }.start();
    }

    protected abstract void specificOnCompletion();

    @Override
    public void onFailure() {
        new Thread() {

            @Override
            public void run() {
                Agent.finishedApplication(appId);
                specificOnFailure();
            }
        }.start();
    }

    protected abstract void specificOnFailure();

    @Override
    public abstract UniqueTaskMonitor getTaskMonitor();


    protected abstract class UniqueTaskMonitor implements TaskMonitor {

        @Override
        public ParameterMonitor getParameterMonitor(int paramId) {
            return AppMonitor.this.taskResults[paramId].getMonitor();
        }

        @Override
        public final void onCreation() {
            specificOnCreation();
        }

        protected abstract void specificOnCreation();

        @Override
        public final void onAccessesProcessed() {
            specificOnAccessesProcessed();
        }

        protected abstract void specificOnAccessesProcessed();

        @Override
        public final void onSchedule() {
            specificOnSchedule();
        }

        protected abstract void specificOnSchedule();

        @Override
        public final void onSubmission() {
            specificOnSubmission();
        }

        protected abstract void specificOnSubmission();

        @Override
        public final void onDataReception() {
            specificOnDataReception();
        }

        protected abstract void specificOnDataReception();

        @Override
        public final void onExecutionStart() {
            specificOnExecutionStart();
        }

        protected abstract void specificOnExecutionStart();

        @Override
        public final void onExecutionStartAt(long t) {
            specificOnExecutionStartAt(t);
        }

        protected abstract void specificOnExecutionStartAt(long t);

        @Override
        public final void onExecutionEnd() {
            specificOnExecutionEnd();
        }

        protected abstract void specificOnExecutionEnd();

        @Override
        public final void onExecutionEndAt(long t) {
            specificOnExecutionEndAt(t);
        }

        protected abstract void specificOnExecutionEndAt(long t);

        @Override
        public final void onAbortedExecution() {
            specificOnAbortedExecution();
        }

        protected abstract void specificOnAbortedExecution();

        @Override
        public final void onErrorExecution() {
            specificOnErrorExecution();
        }

        protected abstract void specificOnErrorExecution();

        @Override
        public final void onFailedExecution() {
            specificOnFailedExecution();
        }

        protected abstract void specificOnFailedExecution();

        @Override
        public final void onSuccesfulExecution() {
            specificOnSuccessfulExecution();
        }

        protected abstract void specificOnSuccessfulExecution();

        @Override
        public final void onCancellation() {
            specificOnCancellation();
            AppMonitor.this.onCancellation();
        }

        protected abstract void specificOnCancellation();

        @Override
        public final void onException(COMPSsException e) {
            AppMonitor.this.exception = e;
            specificOnException(e);
            AppMonitor.this.onException(e);
        }

        protected abstract void specificOnException(COMPSsException e);

        @Override
        public final void onCompletion() {
            specificOnCompletion();
            AppMonitor.this.onCompletion();
        }

        protected abstract void specificOnCompletion();

        @Override
        public void onFailure() {
            specificOnFailure();
            AppMonitor.this.onFailure();
        }

        protected abstract void specificOnFailure();

    }

    /**
     * Class representing a result of a task execution.
     */
    public static class TaskResult {

        private final String externalDataId;


        public TaskResult(String externalDataId, AppMonitor monitor) {
            this.externalDataId = externalDataId;
        }

        public boolean isCollective() {
            return false;
        }

        public ParameterMonitor getMonitor() {
            return new ParameterUpdater();
        }

        /**
         * Returns all the locstions where to find the task result.
         *
         * @return returns a list of all the locations where to find the task result.
         */
        public Collection<RemoteDataLocation> getLocations() {
            if (this.externalDataId != null) {
                LogicalData ld = Comm.getData(this.externalDataId);
                if (ld != null) {
                    return createRDLfromLD(ld);
                }
            }
            return null;
        }

        private Collection<RemoteDataLocation> createRDLfromLD(LogicalData ld) {

            Collection<RemoteDataLocation> locations = new ArrayList<>();

            boolean isLocal = false;
            for (DataLocation loc : ld.getLocations()) {
                boolean localLocation = isLocal(loc);
                isLocal = isLocal || localLocation;
                RemoteDataLocation rdl = createRemoteDLFromLocation(loc, localLocation);
                if (rdl != null) {
                    locations.add(rdl);
                }
            }
            // this is done to prevent ConcurrentModificationException when iterating ld.getKnownAlias()
            // and contain the performance loss to the less frequent part of the code (this one)
            boolean done = false;
            if (isLocal) {
                Collection<RemoteDataLocation> localLocations;
                while (!done) {
                    localLocations = new ArrayList<>();
                    try {
                        for (String alias : ld.getKnownAlias()) {
                            localLocations.add(new PrivateRemoteDataLocation(null, alias));
                        }
                    } catch (ConcurrentModificationException cme) {
                        LOGGER.warn("Logical data was modified while constructing it's remote data location"
                            + " to send as a result");
                    }
                    locations.addAll(localLocations);
                    done = true;
                }
            }
            return locations;
        }

        private RemoteDataLocation createRemoteDLFromLocation(DataLocation loc, boolean isLocal) {
            RemoteDataLocation rdl = null;
            switch (loc.getType()) {
                case PRIVATE:
                    if (!isLocal) {
                        for (MultiURI uri : loc.getURIs()) {
                            Resource<?, ?> hostResource = createRemoteResourceFromResource(uri.getHost());
                            if (hostResource != null) {
                                String pathInHost = uri.getPath();
                                rdl = new PrivateRemoteDataLocation(hostResource, pathInHost);
                            }
                        }
                    }
                    break;
                case SHARED:
                    SharedDisk sd = loc.getSharedDisk();
                    String diskName = sd.getName();
                    Map<es.bsc.compss.types.resources.Resource, String> sdMountpoints = sd.getAllMountpoints();
                    SharedRemoteDataLocation.Mountpoint[] srdlMountpoints;
                    srdlMountpoints = new SharedRemoteDataLocation.Mountpoint[sdMountpoints.size()];
                    int i = 0;
                    for (Map.Entry<es.bsc.compss.types.resources.Resource, String> sdMp : sdMountpoints.entrySet()) {
                        Resource r = createRemoteResourceFromResource(sdMp.getKey());
                        String mountpoint = sdMp.getValue();
                        srdlMountpoints[i++] = new SharedRemoteDataLocation.Mountpoint(r, mountpoint);
                    }
                    rdl = new SharedRemoteDataLocation(diskName, loc.getPath(), srdlMountpoints);
                    break;
                default:
            }
            return rdl;
        }

        private Resource<?, ?> createRemoteResourceFromResource(es.bsc.compss.types.resources.Resource res) {
            COMPSsNode node = res.getNode();

            String name = node.getName();
            String adaptor = node.getAdaptor();
            Object project = node.getProjectProperties();
            Object resources = node.getResourcesProperties();

            if (resources == null) {
                return null;
            } else {
                Resource<?, ?> remoteResource = new Resource<>(name, null, adaptor, project, resources);
                return remoteResource;
            }

        }

        private boolean isLocal(DataLocation dl) {
            for (es.bsc.compss.types.resources.Resource host : dl.getHosts()) {
                if (host == Comm.getAppHost()) {
                    return true;
                }
            }
            return false;
        }


        public class ParameterUpdater implements ParameterMonitor {

            @Override
            public void onCreation(DataType type, String dataName) {
                if (dataName.compareTo(externalDataId) != 0) {
                    try {
                        Comm.linkData(externalDataId, dataName);
                    } catch (CommException ce) {
                        ErrorManager.error("Could not link " + externalDataId + " and " + dataName, ce);
                    }
                }
            }
        }
    }

    /**
     * Class representing a result of a task execution.
     */
    public static class CollectionTaskResult extends TaskResult {

        private final TaskResult[] subElements;


        public CollectionTaskResult(String externalDataId, TaskResult[] subResults, AppMonitor monitor) {
            super(externalDataId, monitor);
            this.subElements = subResults;
        }

        @Override
        public boolean isCollective() {
            return true;
        }

        public TaskResult[] getSubelements() {
            return this.subElements;
        }

        @Override
        public ParameterMonitor getMonitor() {
            return new CollectionParameterUpdater();
        }


        public class CollectionParameterUpdater extends ParameterUpdater implements ParameterCollectionMonitor {

            @Override
            public ParameterMonitor getParameterMonitor(int i) {
                return CollectionTaskResult.this.subElements[i].getMonitor();
            }
        }
    }
}
