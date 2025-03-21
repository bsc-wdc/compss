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
package es.bsc.compss.types.data.operation.copy;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.OperationEndState;
import es.bsc.compss.util.FileOpsManager;
import es.bsc.compss.util.FileOpsManager.FileOpListener;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;


public abstract class Copy extends DataOperation {

    protected final LogicalData srcData;
    protected final DataLocation srcLoc;
    protected final LogicalData tgtData;
    protected DataLocation tgtLoc;
    protected final Transferable reason;

    private final SiblingCopiesManager siblingHandler;


    /**
     * Data Copy Constructor.
     *
     * @param srcData source logical data
     * @param prefSrc preferred source data location
     * @param prefTgt preferred target data location
     * @param tgtData target logical data
     * @param reason Transfer reason
     * @param listener listener to notify changes on the copy state
     */
    public Copy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        super(srcData, listener);
        this.srcData = srcData;
        this.srcLoc = prefSrc;
        this.tgtData = tgtData;
        this.tgtLoc = prefTgt;
        this.reason = reason;
        this.siblingHandler = new SiblingCopiesManager();
    }

    /**
     * Returns the type of the data value being copied.
     *
     * @return type of value being copied
     */
    public DataType getType() {
        return reason.getType();
    }

    /**
     * Returns the source data.
     *
     * @return
     */
    public LogicalData getSourceData() {
        return this.srcData;
    }

    /**
     * Returns the preferred location of the source data.
     *
     * @return
     */
    public DataLocation getPreferredSource() {
        return this.srcLoc;
    }

    public void setTargetLoc(DataLocation loc) {
        tgtLoc = loc;
    }

    public void setProposedSource(Object source) {
        reason.setDataSource(source);
    }

    /**
     * Returns the target location.
     *
     * @return
     */
    public DataLocation getTargetLoc() {
        return this.tgtLoc;
    }

    /**
     * Returns the target data.
     *
     * @return
     */
    public LogicalData getTargetData() {
        return this.tgtData;
    }

    /**
     * Sets a new final target data.
     *
     * @param targetAbsolutePath Absolute path
     */
    public void setFinalTarget(String targetAbsolutePath) {
        if (DEBUG) {
            LOGGER.debug(" Setting StorageCopy final target to : " + targetAbsolutePath);
        }
        this.reason.setDataTarget(targetAbsolutePath);
    }

    /**
     * Returns whether the target data is registered or not.
     *
     * @return
     */
    public boolean isRegistered() {
        return this.tgtData != null;
    }

    public String getFinalTarget() {
        return reason.getDataTarget();
    }

    /**
     * Register a copy with the same data origin and a different target location.
     *
     * @param targetPath path were the data should be stored
     * @param targetLocation DataLocation where the data should be stored
     * @param targetData LogicalData to register the new location
     * @param reason Transfer reason.
     * @param listener Transfer listener.
     * @throws CompletedCopyException the copy has already finished and the data value may be compromised.
     */
    public void addSiblingCopy(String targetPath, DataLocation targetLocation, LogicalData targetData,
        Transferable reason, EventListener listener) throws CompletedCopyException {
        SiblingCopy c = new SiblingCopy(this.srcData, targetPath, targetLocation, targetData, reason, listener);
        this.siblingHandler.addSiblingCopy(c);
    }

    @Override
    public void end(OperationEndState state) {
        String endedPath = reason.getDataTarget();
        this.siblingHandler.finishOriginalCopy(endedPath, false, state, null);
    }

    @Override
    public void end(OperationEndState state, Exception e) {
        String endedPath = reason.getDataTarget();
        this.siblingHandler.finishOriginalCopy(endedPath, true, state, e);
    }

    private void superEnd(OperationEndState state) {
        super.end(state);
    }

    private void superEnd(OperationEndState state, Exception e) {
        super.end(state, e);
    }


    private static final class SiblingCopy {

        private final LogicalData sourceData;
        private final String targetPath;
        private final DataLocation targetLoc;
        private final LogicalData targetData;
        private final Transferable reason;
        private final EventListener listener;


        public SiblingCopy(LogicalData sourceData, String targetPath, DataLocation targetLoc, LogicalData targetData,
            Transferable reason, EventListener listener) {
            this.sourceData = sourceData;
            this.targetPath = targetPath;
            this.targetLoc = targetLoc;
            this.targetData = targetData;
            this.reason = reason;
            this.listener = listener;
        }

        public LogicalData getSourceData() {
            return sourceData;
        }

        public String getTargetPath() {
            return targetPath;
        }

        public DataLocation getTargetLocation() {
            return targetLoc;
        }

        public LogicalData getTargetData() {
            return targetData;
        }

        public Transferable getReason() {
            return reason;
        }

        public EventListener getListener() {
            return listener;
        }

    }

    private static final class SiblingCopyListener implements FileOpListener {

        private final String sourcePath;
        private final SiblingCopy copy;
        private final SiblingCopiesManager groupListener;


        public SiblingCopyListener(String sourcePath, SiblingCopy c, SiblingCopiesManager groupListener) {
            this.sourcePath = sourcePath;
            this.copy = c;
            this.groupListener = groupListener;
        }

        @Override
        public void completed() {
            LogicalData tgtData = copy.getTargetData();
            DataLocation targetLoc = copy.getTargetLocation();
            String targetPath = copy.getTargetPath();
            Transferable reason = copy.getReason();
            if (DEBUG) {
                String name = copy.getSourceData().getName();
                LOGGER.debug("Master local copy " + name + " from " + sourcePath + " to " + targetPath + " done");
            }
            if (tgtData != null) {
                synchronized (tgtData) {
                    tgtData.addLocation(targetLoc);
                }
            }
            reason.setDataTarget(targetPath);
            groupListener.finishedSiblingCopy(copy);
        }

        @Override
        public void failed(IOException e) {
            if (DEBUG) {
                String name = copy.getSourceData().getName();
                String targetPath = copy.getTargetPath();
                LOGGER.debug("Master local copy " + name + " from " + sourcePath + " to " + targetPath + "failed");
            }
            groupListener.failedSiblingCopy(copy, e);
        }

    }

    private final class SiblingCopiesManager {

        // List of copies that need to be done locally after obtaining the value and before notifying its reception
        private final Collection<SiblingCopy> pendingSiblings = new LinkedList<>();
        // Number of pending sibling copies already ordered
        private int pendingSiblingCompletions = 0;
        private boolean originalCopyCompleted = false;

        // Original copy end information
        private String endedPath;
        private boolean notifyWithException = false;
        private OperationEndState state;
        private Exception e;


        public void finishOriginalCopy(String endedPath, boolean notifyWithException, OperationEndState state,
            Exception e) {
            this.notifyWithException = notifyWithException;
            this.state = state;
            this.e = e;
            this.endedPath = endedPath;
            boolean noPendingOps;
            synchronized (this) {
                noPendingOps = pendingSiblings.isEmpty();
                for (SiblingCopy copy : this.pendingSiblings) {
                    orderSiblingCopy(copy);
                }
                pendingSiblings.clear();
                this.originalCopyCompleted = true;
            }
            if (noPendingOps) {
                notifyEnd();
            }
        }

        public synchronized void addSiblingCopy(SiblingCopy copy) throws CompletedCopyException {
            if (originalCopyCompleted) {
                if (pendingSiblingCompletions > 0) {
                    orderSiblingCopy(copy);
                } else {
                    throw new CompletedCopyException();
                }
            } else {
                pendingSiblings.add(copy);
            }
        }

        private void orderSiblingCopy(SiblingCopy c) {
            SiblingCopyListener siblingCopyListener;
            siblingCopyListener = new SiblingCopyListener(endedPath, c, this);
            String targetPath = c.getTargetPath();
            if (DEBUG) {
                LOGGER.debug("Master local copy from " + endedPath + " to " + targetPath);
            }
            pendingSiblingCompletions++;
            FileOpsManager.copyAsync(new File(endedPath), new File(targetPath), siblingCopyListener);
        }

        private synchronized void finishedSiblingCopy(SiblingCopy copy) {
            EventListener finishedCopyListener = copy.getListener();
            Copy.this.addEventListener(finishedCopyListener);
            pendingSiblingCompletions--;
            if (pendingSiblingCompletions == 0) {
                notifyEnd();
            }
        }

        private synchronized void failedSiblingCopy(SiblingCopy copy, Exception e) {
            EventListener failedSiblingListener = copy.getListener();
            failedSiblingListener.notifyFailure(Copy.this, e);
            pendingSiblingCompletions--;
            if (pendingSiblingCompletions == 0) {
                notifyEnd();
            }
        }

        private void notifyEnd() {
            if (notifyWithException) {
                Copy.this.superEnd(state, e);
            } else {
                Copy.this.superEnd(state);
            }
        }
    }
}
