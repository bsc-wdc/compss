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
package es.bsc.compss.gos.master;

import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.comm.Dispatcher;
import es.bsc.compss.gos.master.configuration.GOSConfiguration;
import es.bsc.compss.gos.master.exceptions.GOSException;
import es.bsc.compss.gos.master.monitoring.GOSMonitoring;
import es.bsc.compss.gos.master.sshutils.SSHGlobalHostCollection;
import es.bsc.compss.gos.master.sshutils.SSHHost;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.uri.MultiURI;

import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.RequestQueue;
import es.bsc.compss.util.ThreadPool;
import es.bsc.conn.types.StarterCommand;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GOSAdaptor implements CommAdaptor {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final String ID = GOSAdaptor.class.getCanonicalName();
    private static final String MASTER_USER = System.getProperty("user.name");
    protected static final String THREAD_POOL_ERR = "Error starting pool of threads";
    protected static final String POOL_ERR = "Error deleting pool of threads";
    private RequestQueue<DataOperation> copyQueue;

    private GOSMonitoring gosMonitoring;
    private List<GOSWorkerNode> workerNodes;

    protected ThreadPool pool;

    public SSHGlobalHostCollection hosts;


    public GOSAdaptor() {
    }

    public static String getMasterUser() {
        return MASTER_USER;
    }

    @Override
    public void init() {
        LOGGER.debug("Initializing GOSAdaptor");
        gosMonitoring = new GOSMonitoring(this);
        workerNodes = new LinkedList<>();
        copyQueue = new RequestQueue<>();
        hosts = new SSHGlobalHostCollection();
        pool = new ThreadPool(3, "CopyQueue", new Dispatcher(copyQueue));
        try {
            pool.startThreads();
        } catch (Exception e) {
            ErrorManager.error(THREAD_POOL_ERR, e);
        }
    }

    public void enqueueCopy(GOSCopy copy) {
        copyQueue.enqueue(copy);
    }

    @Override
    public Configuration constructConfiguration(Map<String, Object> projectProperties,
        Map<String, Object> resourcesProperties) {
        GOSConfiguration config = new GOSConfiguration(this, gosMonitoring);
        LOGGER.debug("Constructing GOSConfiguration");
        config.addResourcesProperties(resourcesProperties);
        config.addProjectProperties(projectProperties);
        config.setUser(MASTER_USER);
        return config;
    }

    @Override
    public StarterCommand getStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        String envScriptFromFile, String pythonInterpreterFromFile, int totalCPU, int totalGPU, int totalFPGA,
        int limitOfTasks, String hostId) {
        return null;
    }

    @Override
    public COMPSsWorker initWorker(Configuration config, NodeMonitor monitor) {
        GOSConfiguration gosConfig = (GOSConfiguration) config;
        GOSWorkerNode worker = null;
        // Look if it exists another worker node with the same host
        for (GOSWorkerNode wn : workerNodes) {
            if (gosConfig.getHost().equals(wn.getConfig().getHost())) {
                worker = wn;
                wn.addMonitor(monitor);
                break;
            }
        }
        if (worker == null) {
            worker = new GOSWorkerNode(monitor, gosConfig, this);
            LOGGER.debug("Init GOSWorker named " + gosConfig.getHost());
            workerNodes.add(worker);
        }
        return worker;
    }

    @Override
    public List<DataOperation> getPending() {
        return copyQueue.getQueue();
    }

    @Override
    public void completeMasterURI(MultiURI uri) {
        GOSUri gosUri = new GOSUri(MASTER_USER, uri.getHost(), uri.getPath(), uri.getProtocol());
        uri.setInternalURI(ID, gosUri);
    }

    @Override
    public void stopSubmittedJobs() {
        for (GOSWorkerNode worker : workerNodes) {
            for (GOSJob job : worker.runningJobs.values()) {
                try {
                    job.cancelJob();
                } catch (Exception e) {
                    LOGGER.warn("Exception cancelling job");
                }
            }
        }

    }

    @Override
    public void stop() {
        LOGGER.info("[GOSAdaptor] Starting Stopping process");

        gosMonitoring.end();
        try {
            pool.stopThreads();
        } catch (Exception e) {
            LOGGER.error(POOL_ERR, e);
        }
        Semaphore sem = new Semaphore(0);
        ShutdownListener sl = new ShutdownListener(sem);
        for (GOSWorkerNode worker : workerNodes) {
            LOGGER.info("[GOSAdaptor] Stopping GOSWorker " + worker.getName());
            sl.addOperation();
            worker.stop(sl);
        }

        sl.enable();
        try {
            sem.acquire();
        } catch (Exception e) {
            LOGGER.error("ERROR: Exception raised on worker shutdown");
        }
        LOGGER.debug("[GOSAdaptor] Closing monitoring thread.");
        hosts.releaseAllResources();
        gosMonitoring.end();
        boolean alive = true;
        while (gosMonitoring != null && alive) {
            alive = gosMonitoring.isMonitoringThreadAlive();
        }
        LOGGER.debug("[GOSAdaptor] Stopping process done.");
    }

    public SSHHost getHost(String user, String host) {
        return hosts.getHost(user, host);
    }

    public SSHGlobalHostCollection getHosts() {
        return hosts;
    }

}
