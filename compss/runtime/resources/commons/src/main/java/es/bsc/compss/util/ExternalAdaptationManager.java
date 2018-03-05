package es.bsc.compss.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.MasterResource;
import es.bsc.compss.types.resources.MethodWorker;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ExternalAdaptationManager extends Thread {

    private static final Logger RUNTIME_LOGGER = LogManager.getLogger(Loggers.RM_COMP);
    private static final Logger RESOURCES_LOGGER = LogManager.getLogger(Loggers.RESOURCES);
    private static final boolean DEBUG = RUNTIME_LOGGER.isDebugEnabled();

    private static final String LOG_PREFIX = "[ExternalAdaptation] ";
    private static final String CREATE = "CREATE";
    private static final String STOP = "STOP";
    private static final String REMOVE = "REMOVE";
    private static final String ACK = "ACK";

    private final String adaptationDir;
    private final String commandPipe;
    private final String resultPipe;

    private boolean running = false;
    private boolean canBeStarted = false;


    public ExternalAdaptationManager() {
        MasterResource master = Comm.getAppHost();
        if (master == null) {
            // Testing purposes
            master = new MasterResource();
        }

        this.adaptationDir = master.getAppLogDirPath() + "adaptation" + File.separator;
        this.commandPipe = this.adaptationDir + "command_pipe";
        this.resultPipe = this.adaptationDir + "result_pipe";
        createPipes();
    }

    @Override
    public final void run() {
        if (this.canBeStarted) {
            this.running = true;
            while (this.running) {
                String[] params = readPipe(this.commandPipe);
                if (params != null) {
                    String action = params[0];
                    switch (action) {
                        case CREATE:
                            manageCreate(params);
                            break;
                        case STOP:
                            removePipes();
                            break;
                        case REMOVE:
                            manageRemove(params);
                            break;
                        default:
                            RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Incorrect command.");
                            writePipe(this.resultPipe, "ERROR: Incorrect command");
                            break;
                    }
                } else {
                    RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Read command is null.");
                    // Not sure if error should be written in the result pipe in this case.
                    writePipe(this.resultPipe, "ERROR: Read command is null");
                }
            }
        } else {
            RUNTIME_LOGGER.warn(LOG_PREFIX + "External Adaptation manager not started");
        }
    }

    /**
     * Shutdown the adaptation manager
     * 
     */
    public void shutdown() {
        if (this.canBeStarted) {
            writePipe(this.commandPipe, STOP);
            this.running = false;
        }
    }

    private void createPipes() {
        if (!new File(this.adaptationDir).mkdir()) {
            ErrorManager.error("ERROR: Error creating adaptation dir");
        }
        String[] command = new String[] { "mkfifo", commandPipe, resultPipe };
        try {
            Process process = new ProcessBuilder(command).inheritIO().start();
            int result = process.waitFor();
            if (result != 0) {
                ErrorManager.error("Creating external adaptation pipes failed");
            } else {
                RUNTIME_LOGGER.info(LOG_PREFIX + "Adaptation pipes created succesfully");
                this.canBeStarted = true;
            }
        } catch (Exception e) {
            ErrorManager.error("Exception crating external adaptation pipes", e);
        }
    }

    private void removePipes() {
        if (!new File(this.commandPipe).delete()) {
            RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Cannot erase command pipe");
        }
        if (!new File(this.resultPipe).delete()) {
            RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Cannot erase result pipe");
        }
        if (!new File(this.adaptationDir).delete()) {
            RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Cannot erase adaptation directory");
        }
    }

    /**
     * Manages remove messages
     * 
     * @param params
     */
    private void manageRemove(String[] params) {
        // Cloud provider message : (cloudProvider name, ip) Connector will destroy the resource
        // No cloud creation message: (ip ) Just stop the runtime.
        if (params.length == 3) { // Cloud Creation
            String providerName = params[1];
            String ip = params[2];
            cloudRemove(providerName, ip);
        } else if (params.length == 2) { // Node stop
            String ip = params[1];
            normalRemove(ip);
        } else {
            RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Number of parameters is incorrect (" + params.length + ")");
            writePipe(resultPipe, "ERROR: Number of parameters is incorrect (" + params.length + ")");

        }
    }

    /**
     * Manages create messages
     * 
     * @param params
     */
    private void manageCreate(String[] params) {
        // Cloud creation message : (cloudProvider name, cloudInstanceType name, image name) Connector will create the
        // resource
        // No cloud creation message: (ip, description) Node must be up and ready.
        if (params.length == 4) { // Cloud Creation
            String providerName = params[1];
            String typeName = params[2];
            String imageName = params[3];
            cloudCreation(providerName, typeName, imageName);
        } else if (params.length == 3) { // Node start
            String resourceName = params[1];
            String description = params[2];
            normalCreation(resourceName, description);
        } else {
            RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Number of parameters is incorrect (" + params.length + ")");
            writePipe(resultPipe, "ERROR: Number of parameters is incorrect (" + params.length + ")");

        }
    }

    private void normalRemove(String name) {
        MethodWorker w = (MethodWorker) ResourceManager.getWorker(name);
        if (w != null) {
            // TODO: ResourceManager.reduceWholeWorker(w);
            RUNTIME_LOGGER.info(LOG_PREFIX + "TODO: Resource " + name + " removed.");
            writePipe(resultPipe, ACK);
        } else {
            RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Resource " + name + " not found.");
            writePipe(resultPipe, "ERROR: Error creating resource " + name + " not found.");
        }
    }

    private void cloudRemove(String providerName, String name) {
        if (providerName != null && name != null) {
            CloudProvider cp = ResourceManager.getCloudProvider(providerName);
            if (cp != null) {
                CloudMethodWorker cmw = cp.getHostedWorker(name);
                if (cmw != null) {
                    ResourceManager.reduceCloudWorker(cmw, cmw.getDescription());
                    RUNTIME_LOGGER.info(LOG_PREFIX + "Submited external request for removing " + name + " in " + providerName);
                    writePipe(resultPipe, ACK);
                } else {
                    RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: resource " + name + " not found in " + providerName);
                    writePipe(resultPipe, "ERROR: Error creating resource " + name + " not found in " + providerName);
                }
            } else {
                RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Provider " + providerName + " not found.");
                writePipe(resultPipe, "ERROR: Provider " + providerName + " not found");

            }
        } else {
            RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: One of the parameters is incorrect (" + name + "," + providerName + ")");
            writePipe(resultPipe, "ERROR: One of the parameters is incorrect (" + name + "," + providerName + ")");
        }

    }

    private void normalCreation(String name, String description) {
        // TODO
        RUNTIME_LOGGER.info(LOG_PREFIX + "TODO: Resource " + name + " created.");
        writePipe(resultPipe, ACK);
    }

    private void cloudCreation(String providerName, String typeName, String imageName) {
        if (providerName != null && typeName != null && imageName != null) {
            CloudProvider cp = ResourceManager.getCloudProvider(providerName);
            if (cp != null) {
                CloudImageDescription imageDescription = cp.getImage(imageName);
                CloudInstanceTypeDescription typeDescription = cp.getInstanceType(typeName);
                CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(typeDescription, imageDescription);
                ResourceCreationRequest rcr = cp.requestResourceCreation(cmrd);
                if (rcr != null) {
                    RUNTIME_LOGGER.info(
                            LOG_PREFIX + "Submited external request for creating (" + typeName + ", " + imageName + ") in " + providerName);
                    rcr.print(RESOURCES_LOGGER, DEBUG);
                    writePipe(resultPipe, ACK);
                } else {
                    RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Creating resource (" + typeName + ", " + imageName + ") in " + providerName);
                    writePipe(resultPipe, "ERROR: Error creating resource(" + typeName + ", " + imageName + ") in " + providerName);
                }
            } else {
                RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Provider " + providerName + " not found.");
                writePipe(resultPipe, "ERROR: Provider " + providerName + " not found");

            }
        } else {
            RUNTIME_LOGGER.error(
                    LOG_PREFIX + "ERROR: One of the parameters is incorrect (" + typeName + ", " + imageName + "," + providerName + ")");
            writePipe(resultPipe, "ERROR: One of the parameters is incorrect (" + typeName + ", " + imageName + "," + providerName + ")");
        }

    }

    private void writePipe(String pipe, String result) {
        try (FileOutputStream output = new FileOutputStream(pipe, true);) {
            output.write(result.getBytes());
            output.flush();
            output.close();
        } catch (Exception e) {
            RUNTIME_LOGGER.debug(LOG_PREFIX + "Error on adaptation result pipe write. ", e);
        }

    }

    private String[] readPipe(String pipe) {
        try (FileInputStream input = new FileInputStream(pipe); BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
            String line = reader.readLine();
            if (line != null) {
                String[] result = line.split(" ");
                return result;
            } else {
                RUNTIME_LOGGER.error(LOG_PREFIX + "ERROR: Line read on adapatation command pipe is null.");
                return null;
            }
        } catch (Exception e) {
            RUNTIME_LOGGER.error(LOG_PREFIX + "Error on adapatation command pipe read.", e);
            return null;
        }
    }

}
