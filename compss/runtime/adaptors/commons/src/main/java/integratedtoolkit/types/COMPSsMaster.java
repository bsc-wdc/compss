package integratedtoolkit.types;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.comm.CommAdaptor;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.data.operation.Copy;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.data.operation.SafeCopyListener;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.concurrent.Semaphore;


public class COMPSsMaster extends COMPSsNode {

    protected static final String ERROR_UNKNOWN_HOST = "ERROR: Cannot determine the IP address of the local host";

    private final String name;

    //private final String workingDirectory;
    public COMPSsMaster() {
        super();
        // Initializing host attributes
        String hostName = "";
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            hostName = localHost.getCanonicalHostName();
        } catch (UnknownHostException e) {
            ErrorManager.error("ERROR_UNKNOWN_HOST: " + e.getLocalizedMessage());
        }
        name = hostName;

    }

    @Override
    public void start() {
        //Do nothing.
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setInternalURI(URI u) {
        for (CommAdaptor adaptor : Comm.getAdaptors().values()) {
            adaptor.completeMasterURI(u);
        }
    }

    @Override
    public void stop(ShutdownListener sl) {
        //NO need to do anything
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason, DataOperation.EventListener listener) {
        for (Resource targetRes : target.getHosts()) {
            COMPSsNode node = targetRes.getNode();
            if (node != this) {
                try {
                    node.obtainData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    //Can not copy the file. 
                    //Cannot receive the file, try with the following
                    continue;
                }
                return;
            }

        }
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason, DataOperation.EventListener listener) {
        //Check if data is in memory
        if (ld.isInMemory()) {
            try {
                Serializer.serialize(ld.getValue(), target.getPath());
                if (tgtData != null) {
                    tgtData.addLocation(target);
                }
                logger.debug("Object in memory set dataTarget "+ target.getPath());
                reason.setDataTarget(target.getPath());
                listener.notifyEnd(null);
                return;
            } catch (IOException ex) {
                ErrorManager.warn("Error copying file from memory to " + target.getPath(), ex);
            }
        }
        if (debug) {
            logger.debug("Data " + ld.getName() + " not in memory. Checking if there is a copy to the master in progres");
        }

        ld.lockHostRemove();
        //Check if there are current copies in progress

        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();

        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            for (Copy copy : copiesInProgress) {
                if (copy != null) {
                    if (copy.getTargetLoc() != null
                            && copy.getTargetLoc().getHosts()
                            .contains(Comm.appHost)) {
                        if (debug) {
                            logger.debug("Copy in progress tranfering "
                                    + ld.getName()
                                    + "to master. Waiting for finishing");
                        }
                        waitForCopyTofinish(copy);
						try {
							if (debug) {
								logger.debug("Master local copy "
										+ ld.getName() + " from "
										+ copy.getFinalTarget() + " to "
										+ target.getPath());
							}
							Files.copy(
									(new File(copy.getFinalTarget())).toPath(),
									new File(target.getPath()).toPath(),
									StandardCopyOption.REPLACE_EXISTING);
							if (tgtData != null) {
								tgtData.addLocation(target);
							}
							logger.debug("File copied set dataTarget "
									+ target.getPath());
							reason.setDataTarget(target.getPath());

							listener.notifyEnd(null);
							ld.releaseHostRemoveLock();
                            return;
                        } catch (IOException ex) {
                            ErrorManager.warn("Error master local copying file "+ copy.getFinalTarget()+ " from master to "
                                    + target.getPath() + " with replacing", ex);
                        }

                    } else if (copy.getTargetData() != null
                            && copy.getTargetData().getAllHosts()
                            .contains(Comm.appHost)) {
                        waitForCopyTofinish(copy);
                        try {

                        	if (debug) {
                        		logger.debug("Master local copy "
                        				+ ld.getName() + " from "
                        				+ copy.getFinalTarget() + " to "
                        				+ target.getPath());
                        	}
                        	Files.copy(
                        			(new File(copy.getFinalTarget())).toPath(),
                        			new File(target.getPath()).toPath(),
                        			StandardCopyOption.REPLACE_EXISTING);
                        	if (tgtData != null) {
                        		tgtData.addLocation(target);
                        	}
                        	logger.debug("File copied set dataTarget "+ target.getPath());
                        	reason.setDataTarget(target.getPath());

                        	listener.notifyEnd(null);
                        	ld.releaseHostRemoveLock();
                        	return;
                        } catch (IOException ex) {
                        	ErrorManager.warn("Error master local copy from "+ copy.getFinalTarget()+ " to "
                        			+ target.getPath() + " with replacing", ex);
                        }
                    } else {
                        if (debug) {
                            logger.debug("Current copies are not transfering "
                                    + ld.getName()
                                    + " to master. Ignoring at this moment");
                        }
                    }
                }
            }
        }
        if (debug) {
            logger.debug("Checking if " + ld.getName() + " is at master ("+Comm.appHost+").");
        }
        //Checking if in master
        for (URI u : ld.getURIs()) {
        	logger.debug(ld.getName() + " is at "+ u.toString() + "("+ u.getHost()+")");
            if (u.getHost() == Comm.appHost) {
            	try {
            		if (debug) {
            			logger.debug("Master local copy " + ld.getName() + " from " + u.getHost().getName() + " to " + target.getPath());
            		}
            		Files.copy((new File(u.getPath())).toPath(), new File(target.getPath()).toPath(), StandardCopyOption.REPLACE_EXISTING);
            		if (tgtData != null) {
            			tgtData.addLocation(target);
            		}
            		logger.debug("File copied set dataTarget "+ target.getPath());
            		reason.setDataTarget(target.getPath());

            		listener.notifyEnd(null);
            		ld.releaseHostRemoveLock();
            		return;
                } catch (IOException ex) {
                    ErrorManager.warn("Error master local copy file from "+u.getPath()+" to " + target.getPath() + " with replacing", ex);
                }
            } else {
                if (debug) {
                    logger.debug("Data " + ld.getName() + " copy in " + u.getHost() + " not evaluated now");
                }
            }

        }
        if (source != null) {
            for (Resource sourceRes : source.getHosts()) {
                COMPSsNode node = sourceRes.getNode();
                if (node != this) {
                    try {
                        if (debug) {
                            logger.debug("Sending data " + ld.getName() + " from " + source.getPath() + " to " + target.getPath());
                        }
                        node.sendData(ld, source, target, tgtData, reason, listener);
                    } catch (Exception e) {
                        ErrorManager.warn("Not posible to sending data master to " + target.getPath(), e);
                        continue;
                    }
                    logger.debug("Data " + ld.getName() + " sent.");
                    ld.releaseHostRemoveLock();
                    return;
                } else {
                    try {
                        if (debug) {
                            logger.debug("Local copy " + ld.getName() + " from " + source.getPath() + " to " + target.getPath());
                        }
                        //URI u = source.getURIInHost(sourceRes);
                        Files.copy(new File(source.getPath()).toPath(), new File(target.getPath()).toPath(), StandardCopyOption.REPLACE_EXISTING);
                        logger.debug("File copied set dataTarget "+ target.getPath());
                    	reason.setDataTarget(target.getPath());
                        listener.notifyEnd(null);
                        ld.releaseHostRemoveLock();
                        return;

                    } catch (IOException ex) {
                        ErrorManager.warn("Error master local copy file from " + source.getPath() + " to " + target.getPath(), ex);
                    }
                }
            }
        } else {
            logger.debug("Source data location is null. Trying other alternatives");
        }
        for (Resource sourceRes : ld.getAllHosts()) {
            COMPSsNode node = sourceRes.getNode();
            if (node != this) {
                try {
                    logger.debug("Sending data " + ld.getName() + " from " + sourceRes.getName() + " to " + target.getPath());
                    node.sendData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    logger.error("Error: exception sending data", e);
                    continue;
                }
                logger.debug("Data " + ld.getName() + " sent.");
                ld.releaseHostRemoveLock();
                return;
            } else {
                if (debug) {
                    logger.debug("Data " + ld.getName() + " copy in " + sourceRes.getName() + " not evaluated now. Should have been evaluated before");
                }
            }
        }

        ErrorManager.warn("Error file " + ld.getName() + " not transferred to " + target.getPath());
        ld.releaseHostRemoveLock();
    }

    private void waitForCopyTofinish(Copy copy) {
        Semaphore sem = new Semaphore(0);
        SafeCopyListener currentCopylistener = new SafeCopyListener(sem);
        copy.addEventListener(currentCopylistener);
        currentCopylistener.addOperation();
        currentCopylistener.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            ErrorManager.warn("Error waiting for files in resource " + getName() + " to get saved");
        }
        if (debug) {
            logger.debug("Copy " + copy.getName() +"(id: "+copy.getId()+") is finished");
        }
        
    }

    @Override
    public Job<?> newJob(int taskId, TaskParams taskParams, Implementation<?> impl, Resource res, Job.JobListener listener) {
        //Cannot run jobs
        return null;
    }

    @Override
    public String getCompletePath(DataType type, String name) {
        switch (type) {
            case FILE_T:
                return Comm.appHost.getTempDirPath() + name;
            case OBJECT_T:
                return name;
            default:
                return null;
        }
    }

    @Override
    public void deleteTemporary() {
        File dir = new File(Comm.appHost.getTempDirPath());
        for (File f : dir.listFiles()) {
            deleteFolder(f);
        }
    }

    private void deleteFolder(File folder) {
        if (folder != null) {
            if (folder.isDirectory()) {
                for (File f : folder.listFiles()) {
                    deleteFolder(f);
                }
            }
            folder.delete();
        }
    }

    public void generatePackage() {
        // Should not be executed on a COMPSsMaster
    }

    public void generateWorkersDebugInfo() {
        // Should not be executed on a COMPSsMaster
    }

}
