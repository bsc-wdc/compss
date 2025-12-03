/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.components.impl.socketserver;

import es.bsc.compss.api.Workflow;
import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.components.impl.socketserver.ipc.ConnectionHandler;
import es.bsc.compss.components.impl.socketserver.ipc.Server;
import es.bsc.compss.executor.external.commands.ExternalCommand.CommandType;
import es.bsc.compss.executor.external.piped.commands.AccessedFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.BarrierPipeCommand;
import es.bsc.compss.executor.external.piped.commands.BarrierTaskGroupPipeCommand;
import es.bsc.compss.executor.external.piped.commands.CancelTaskGroupPipeCommand;
import es.bsc.compss.executor.external.piped.commands.CloseFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.CloseTaskGroupPipeCommand;
import es.bsc.compss.executor.external.piped.commands.CompssExceptionPipeCommand;
import es.bsc.compss.executor.external.piped.commands.DeleteFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.DeleteObjectPipeCommand;
import es.bsc.compss.executor.external.piped.commands.EndTaskPipeCommand;
import es.bsc.compss.executor.external.piped.commands.ExecuteNestedTaskPipeCommand;
import es.bsc.compss.executor.external.piped.commands.GetAppDirPipeCommand;
import es.bsc.compss.executor.external.piped.commands.GetDirectoryPipeCommand;
import es.bsc.compss.executor.external.piped.commands.GetFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.GetMasterWorkingDirPipeCommand;
import es.bsc.compss.executor.external.piped.commands.GetObjectPipeCommand;
import es.bsc.compss.executor.external.piped.commands.NewBarrierPipeCommand;
import es.bsc.compss.executor.external.piped.commands.NoMoreTasksPipeCommand;
import es.bsc.compss.executor.external.piped.commands.OpenFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.OpenTaskGroupPipeCommand;
import es.bsc.compss.executor.external.piped.commands.PipeCommand;
import es.bsc.compss.executor.external.piped.commands.RegisterCEPipeCommand;
import es.bsc.compss.executor.external.piped.commands.SynchPipeCommand;
import es.bsc.compss.executor.external.piped.exceptions.UnknownCommandException;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.worker.COMPSsException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * UNIX-domain socket server that exposes COMPSs runtime services to external workers. It receives pipe-based commands,
 * translates them into runtime API calls, and streams responses back to the client.
 */
public class SocketServer extends Server {

    private static final Logger LOGGER = LogManager.getLogger(SocketServer.class);

    private static final String LINE_SEPARATOR_REGEX = "\\r?\\n";

    private final COMPSsRuntimeImpl runtime;


    /**
     * Creates a server listening on the given socket path.
     *
     * @param runtime COMPSs runtime instance backing the server operations.
     * @param socketPath UNIX-domain socket file to bind.
     */
    public SocketServer(COMPSsRuntimeImpl runtime, String socketPath) {
        super(socketPath);
        this.runtime = runtime;
    }

    /**
     * Builds a connection handler for a freshly accepted client.
     *
     * @return connection handler instance.
     */
    @Override
    public ConnectionHandler onNewClient() {

        return new CompssConnection();
    }


    /**
     * Connection handler that parses incoming command streams and triggers the matching runtime actions.
     */
    private class CompssConnection extends ConnectionHandler {

        private final Workflow wf;
        private final long appId;


        public CompssConnection() {
            this.wf = runtime.registerWorkflow(null, null);
            this.appId = this.wf.getId();
        }

        /**
         * Logs the arrival of a new socket client.
         */
        @Override
        public void onEstablish() {
            LOGGER.debug("New socket client connected");
        }

        /**
         * Splits the received payload into lines and forwards each command line for processing.
         *
         * @param message buffer containing one or more textual commands.
         */
        @Override
        public void onMessageReception(ByteBuffer message) {
            byte[] data = new byte[message.remaining()];
            message.get(data);
            String payload = new String(data, StandardCharsets.UTF_8);
            String[] lines = payload.split(LINE_SEPARATOR_REGEX);
            for (String rawLine : lines) {
                if (rawLine == null || rawLine.trim().isEmpty()) {
                    continue;
                }
                try {
                    processCommand(rawLine);
                } catch (Exception e) {
                    LOGGER.error("Error processing command '{}'", rawLine, e);
                    sendCompssException("Socket server failure: " + e.getMessage());
                }
            }
        }

        /**
         * Performs a final barrier and deregisters the application when the connection closes.
         */
        @Override
        public void onClose() {
            LOGGER.debug("Socket client disconnected");

            try {
                this.wf.barrier();
            } catch (Exception e) {
                LOGGER.warn("Barrier failed while closing connection", e);
            } finally {
                this.wf.deregister();
            }
        }

        /**
         * Parses the raw command line and dispatches it to the appropriate handler.
         *
         * @param rawLine textual command.
         * @throws IOException if the response cannot be sent.
         * @throws COMPSsException if the runtime reports an error.
         * @throws UnknownCommandException when the command tag is not supported.
         */
        private void processCommand(String rawLine) throws IOException, COMPSsException, UnknownCommandException {
            PipeCommand command = parseCommand(rawLine);
            if (command == null) {
                return;
            }
            CommandType type = command.getType();
            LOGGER.debug("Processing command {} -> {}", type, rawLine);
            switch (type) {
                case REGISTER_CE:
                    handleRegisterCoreElement((RegisterCEPipeCommand) command);
                    break;
                case EXECUTE_NESTED_TASK:
                    handleExecuteNestedTask((ExecuteNestedTaskPipeCommand) command);
                    break;
                case ACCESSED_FILE:
                    handleAccessedFile((AccessedFilePipeCommand) command);
                    break;
                case OPEN_FILE:
                    handleOpenFile((OpenFilePipeCommand) command);
                    break;
                case CLOSE_FILE:
                    handleCloseFile((CloseFilePipeCommand) command);
                    break;
                case DELETE_FILE:
                    handleDeleteFile((DeleteFilePipeCommand) command);
                    break;
                case GET_FILE:
                    handleGetFile((GetFilePipeCommand) command);
                    break;
                case GET_DIRECTORY:
                    handleGetDirectory((GetDirectoryPipeCommand) command);
                    break;
                case GET_OBJECT:
                    handleGetObject((GetObjectPipeCommand) command);
                    break;
                case DELETE_OBJECT:
                    handleDeleteObject((DeleteObjectPipeCommand) command);
                    break;
                case BARRIER:
                    handleBarrier();
                    break;
                case BARRIER_NEW:
                    handleBarrierNew((NewBarrierPipeCommand) command);
                    break;
                case BARRIER_GROUP:
                    handleBarrierGroup((BarrierTaskGroupPipeCommand) command);
                    break;
                case OPEN_TASK_GROUP:
                    handleOpenTaskGroup((OpenTaskGroupPipeCommand) command);
                    break;
                case CLOSE_TASK_GROUP:
                    handleCloseTaskGroup((CloseTaskGroupPipeCommand) command);
                    break;
                case CANCEL_TASK_GROUP:
                    handleCancelTaskGroup((CancelTaskGroupPipeCommand) command);
                    break;
                case NO_MORE_TASKS:
                    handleNoMoreTasks((NoMoreTasksPipeCommand) command);
                    break;
                case END_TASK:
                    handleEndTask((EndTaskPipeCommand) command);
                    break;
                case COMPSS_EXCEPTION:
                    handleIncomingCompssException((CompssExceptionPipeCommand) command);
                    break;
                case GET_MASTERWORKINGDIR:
                    handleGetMasterWorkingDir((GetMasterWorkingDirPipeCommand) command);
                    break;
                case GET_APPDIR:
                    handleGetAppDir((GetAppDirPipeCommand) command);
                    break;
                default:
                    LOGGER.warn("Unsupported command type {} received on socket server", type);
                    break;
            }
        }

        /**
         * Converts the raw command line into a concrete {@link PipeCommand}.
         *
         * @param rawLine textual representation of the command.
         * @return parsed command or {@code null} if the line is empty.
         * @throws UnknownCommandException when the command type is unknown.
         */
        private PipeCommand parseCommand(String rawLine) throws UnknownCommandException {
            String[] tokens = rawLine.split(" ");
            if (tokens.length == 0) {
                return null;
            }
            String tag = tokens[0].toUpperCase();
            CommandType type = CommandType.valueOf(tag);
            switch (type) {
                case REGISTER_CE:
                    return new RegisterCEPipeCommand(tokens);
                case EXECUTE_NESTED_TASK:
                    return new ExecuteNestedTaskPipeCommand(rawLine, tokens);
                case ACCESSED_FILE:
                    return new AccessedFilePipeCommand(tokens);
                case OPEN_FILE:
                    return new OpenFilePipeCommand(tokens);
                case GET_FILE:
                    return new GetFilePipeCommand(tokens);
                case CLOSE_FILE:
                    return new CloseFilePipeCommand(tokens);
                case DELETE_FILE:
                    return new DeleteFilePipeCommand(tokens);
                case GET_DIRECTORY:
                    return new GetDirectoryPipeCommand(tokens);
                case GET_OBJECT:
                    return new GetObjectPipeCommand(tokens);
                case DELETE_OBJECT:
                    return new DeleteObjectPipeCommand(tokens);
                case BARRIER:
                    return new BarrierPipeCommand(tokens);
                case BARRIER_NEW:
                    return new NewBarrierPipeCommand(tokens);
                case BARRIER_GROUP:
                    return new BarrierTaskGroupPipeCommand(tokens);
                case OPEN_TASK_GROUP:
                    return new OpenTaskGroupPipeCommand(tokens);
                case CLOSE_TASK_GROUP:
                    return new CloseTaskGroupPipeCommand(tokens);
                case CANCEL_TASK_GROUP:
                    return new CancelTaskGroupPipeCommand(tokens);
                case NO_MORE_TASKS:
                    return new NoMoreTasksPipeCommand();
                case END_TASK:
                    return new EndTaskPipeCommand(tokens);
                case COMPSS_EXCEPTION:
                    return new CompssExceptionPipeCommand(tokens);
                case SYNCH:
                    return new SynchPipeCommand();
                case GET_MASTERWORKINGDIR:
                    return new GetMasterWorkingDirPipeCommand();
                case GET_APPDIR:
                    return new GetAppDirPipeCommand();
                default:
                    throw new UnknownCommandException(rawLine);
            }
        }

        /**
         * Registers a core element definition in the runtime.
         *
         * @param cmd command describing the core element.
         */
        private void handleRegisterCoreElement(RegisterCEPipeCommand cmd) {
            runtime.registerCoreElement(cmd.getCESignature(), cmd.getImplSignature(), cmd.getConstraints(),
                cmd.getImplType(), cmd.getImplLocal(), cmd.getImplIO(), cmd.getProlog(), cmd.getEpilog(),
                cmd.getConatainer(), cmd.getTypeArgs());
        }

        /**
         * Executes a nested task requested by the client.
         *
         * @param cmd command describing the task execution.
         * @throws COMPSsException if the runtime reports a failure.
         */
        private void handleExecuteNestedTask(ExecuteNestedTaskPipeCommand cmd) throws COMPSsException {
            switch (cmd.getEntryPoint()) {
                case SIGNATURE:
                    runtime.executeTask(this.appId, cmd.getSignature(), cmd.getOnFailure(), cmd.getTimeOut(),
                        cmd.getPrioritary(), cmd.getNumNodes(), cmd.isReduce(), cmd.getReduceChunkSize(),
                        cmd.isReplicated(), cmd.isDistributed(), cmd.hasTarget(), cmd.getNumReturns(),
                        cmd.getParameterCount(), cmd.getParameters());
                    break;
                case CLASS_METHOD:
                    runtime.executeTask(this.appId, cmd.getMethodClass(), cmd.getOnFailure(), cmd.getTimeOut(),
                        cmd.getMethodName(), cmd.getPrioritary(), cmd.getNumNodes(), cmd.isReduce(),
                        cmd.getReduceChunkSize(), cmd.isReplicated(), cmd.isDistributed(), cmd.hasTarget(),
                        cmd.getNumReturns(), cmd.getParameterCount(), cmd.getParameters());
                    break;
                default:
                    LOGGER.warn("Unsupported nested task entry point {}", cmd.getEntryPoint());
            }
        }

        /**
         * Determines whether a logical file is currently accessed by the runtime.
         *
         * @param cmd command providing the file identifier.
         * @throws IOException if the acknowledgement cannot be sent.
         */
        private void handleAccessedFile(AccessedFilePipeCommand cmd) throws IOException {
            boolean accessed = runtime.isFileAccessed(this.appId, cmd.getFile());
            sendCommand(new SynchPipeCommand(accessed ? "1" : "0"));
        }

        /**
         * Requests the runtime to open a logical file under the given access direction.
         *
         * @param cmd command containing the file identifier and access direction.
         * @throws IOException if the response cannot be sent.
         */
        private void handleOpenFile(OpenFilePipeCommand cmd) throws IOException {
            Direction dir = cmd.getDirection();
            String location = runtime.openFile(this.appId, cmd.getFile(), dir);
            sendCommand(new SynchPipeCommand(location));
        }

        /**
         * Notifies the runtime that the client has finished accessing a logical file.
         *
         * @param cmd command containing the file identifier and access direction.
         */
        private void handleCloseFile(CloseFilePipeCommand cmd) {
            runtime.closeFile(this.appId, cmd.getFile(), cmd.getDirection());
        }

        /**
         * Removes a logical file from the runtime and indicates whether the operation succeeded.
         *
         * @param cmd command containing the file identifier.
         * @throws IOException if the response cannot be sent.
         */
        private void handleDeleteFile(DeleteFilePipeCommand cmd) throws IOException {
            boolean deleted = runtime.deleteFile(this.appId, cmd.getFile());
            sendCommand(new SynchPipeCommand(deleted ? "1" : "0"));
        }

        /**
         * Makes the runtime materialise the requested logical file and acknowledges completion.
         *
         * @param cmd command describing the file to retrieve.
         * @throws IOException if the acknowledgement cannot be sent.
         */
        private void handleGetFile(GetFilePipeCommand cmd) throws IOException {
            runtime.getFile(this.appId, cmd.getFile());
            sendCommand(new SynchPipeCommand());
        }

        /**
         * Synchronises a logical directory from the runtime.
         *
         * @param cmd command describing the directory identifier.
         * @throws IOException if the acknowledgement cannot be sent.
         */
        private void handleGetDirectory(GetDirectoryPipeCommand cmd) throws IOException {
            runtime.getDirectory(this.appId, cmd.getDirectory());
            sendCommand(new SynchPipeCommand());
        }

        /**
         * Retrieves a binding object from the runtime.
         *
         * @param cmd command containing the binding object identifier.
         * @throws IOException if the acknowledgement cannot be sent.
         */
        private void handleGetObject(GetObjectPipeCommand cmd) throws IOException {
            runtime.getBindingObject(this.appId, cmd.getObjectId());
            sendCommand(new SynchPipeCommand());
        }

        /**
         * Returns the runtime master working directory.
         *
         * @param cmd command requesting the directory.
         * @throws IOException if the response cannot be sent.
         */
        private void handleGetMasterWorkingDir(GetMasterWorkingDirPipeCommand cmd) throws IOException {
            String dir = runtime.getTempDir();
            sendCommand(new SynchPipeCommand(dir));
        }

        /**
         * Returns the runtime application directory.
         *
         * @param cmd command requesting the directory.
         * @throws IOException if the response cannot be sent.
         */
        private void handleGetAppDir(GetAppDirPipeCommand cmd) throws IOException {
            String dir = runtime.getApplicationDirectory();
            sendCommand(new SynchPipeCommand(dir));
        }

        /**
         * Removes a binding object and informs the caller about the outcome.
         *
         * @param cmd command describing the binding object identifier.
         * @throws IOException if the response cannot be sent.
         */
        private void handleDeleteObject(DeleteObjectPipeCommand cmd) throws IOException {
            boolean deleted = false;
            deleted = runtime.deleteFile(this.appId, cmd.getObjectId());
            sendCommand(new SynchPipeCommand(deleted ? "1" : "0"));
        }

        /**
         * Waits until all tasks submitted by the application have completed.
         *
         * @throws IOException if the acknowledgement cannot be sent.
         */
        private void handleBarrier() throws IOException {
            this.wf.barrier();
            sendCommand(new SynchPipeCommand());
        }

        /**
         * Performs a barrier while optionally marking that no further tasks will arrive.
         *
         * @param cmd barrier command data.
         * @throws IOException if the acknowledgement cannot be sent.
         */
        private void handleBarrierNew(NewBarrierPipeCommand cmd) throws IOException {
            this.wf.barrier(cmd.isNoMoreTasks());
            sendCommand(new SynchPipeCommand());
        }

        /**
         * Synchronises on a task group and returns either an acknowledgement or an exception to the client.
         *
         * @param cmd command describing the task group.
         * @throws IOException if the acknowledgement or exception cannot be sent.
         */
        private void handleBarrierGroup(BarrierTaskGroupPipeCommand cmd) throws IOException {
            try {
                this.wf.barrierGroup(cmd.getGroupName());
                sendCommand(new SynchPipeCommand());
            } catch (COMPSsException ce) {
                sendCommand(new CompssExceptionPipeCommand(null, ce.getMessage()));
            }
        }

        /**
         * Opens a task group for the current connection.
         *
         * @param cmd command describing the desired task group.
         */
        private void handleOpenTaskGroup(OpenTaskGroupPipeCommand cmd) {
            this.wf.openTaskGroup(cmd.getGroupName(), cmd.isImplicitBarrier());
        }

        /**
         * Closes a previously opened task group.
         *
         * @param cmd command containing the task group name.
         */
        private void handleCloseTaskGroup(CloseTaskGroupPipeCommand cmd) {
            this.wf.closeTaskGroup(cmd.getGroupName());
        }

        /**
         * Cancels a task group and forwards any runtime error back to the client.
         *
         * @param cmd command describing the task group to cancel.
         * @throws IOException if the exception cannot be sent.
         */
        private void handleCancelTaskGroup(CancelTaskGroupPipeCommand cmd) throws IOException {
            try {
                this.wf.cancelTaskGroup(cmd.getGroupName());
                sendCommand(new SynchPipeCommand());
            } catch (COMPSsException ce) {
                sendCommand(new CompssExceptionPipeCommand(null, ce.getMessage()));
            }
        }

        /**
         * Notifies the runtime that the client will not submit additional tasks.
         *
         * @param cmd no-more-tasks command.
         * @throws IOException if the acknowledgement cannot be sent.
         */
        private void handleNoMoreTasks(NoMoreTasksPipeCommand cmd) throws IOException {
            this.wf.noMoreTasks();
            sendCommand(new SynchPipeCommand());
        }

        /**
         * Reports the final state of a task and informs the client about non-zero exit codes.
         *
         * @param cmd end-task command with the task status.
         * @throws IOException if the response cannot be sent.
         */
        private void handleEndTask(EndTaskPipeCommand cmd) throws IOException {
            int exitValue = cmd.getTaskStatus().getExitValue();
            if (exitValue != 0) {
                sendCommand(new CompssExceptionPipeCommand(null, "Task finished with exit value " + exitValue));
            } else {
                sendCommand(new SynchPipeCommand());
            }
        }

        /**
         * Logs an exception generated by the remote component.
         *
         * @param cmd command encapsulating the remote exception.
         */
        private void handleIncomingCompssException(CompssExceptionPipeCommand cmd) {
            LOGGER.warn("Remote component raised COMPSs exception: {}", cmd.getMessage());
        }

        /**
         * Serialises and sends the provided pipe command back to the client.
         *
         * @param command response command.
         * @throws IOException if the message cannot be written to the socket.
         */
        private void sendCommand(PipeCommand command) throws IOException {
            byte[] encoded = (command.getAsString() + "\n").getBytes(StandardCharsets.UTF_8);
            sendMessage(ByteBuffer.wrap(encoded));
        }

        /**
         * Attempts to send an exception message to the client, logging failures locally.
         *
         * @param message exception description to send.
         */
        private void sendCompssException(String message) {
            try {
                sendCommand(new CompssExceptionPipeCommand(null, message));
            } catch (IOException ioe) {
                LOGGER.error("Unable to send COMPSs exception back to client", ioe);
            }
        }
    }

}
// CHECKSTYLE:ON
