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
package es.bsc.compss.nio.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import es.bsc.comm.Connection;
import es.bsc.comm.EventManager;
import es.bsc.comm.InternalConnection;
import es.bsc.comm.Node;
import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.exceptions.ViabilityException;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import es.bsc.comm.stage.Transfer.Direction;
import es.bsc.comm.stage.Transfer.Type;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOHandler;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTaskProfile;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.commands.CommandCancelTask;
import es.bsc.compss.nio.commands.CommandDataReceived;
import es.bsc.compss.nio.commands.CommandExecutorShutdown;
import es.bsc.compss.nio.commands.CommandExecutorShutdownACK;
import es.bsc.compss.nio.commands.CommandNIOTaskDone;
import es.bsc.compss.nio.commands.CommandNewTask;
import es.bsc.compss.nio.commands.CommandRemoveObsoletes;
import es.bsc.compss.nio.commands.CommandShutdown;
import es.bsc.compss.nio.commands.CommandShutdownACK;
import es.bsc.compss.nio.commands.tracing.CommandGenerateAnalysisFiles;
import es.bsc.compss.nio.commands.tracing.CommandGenerateAnalysisFilesDone;
import es.bsc.compss.nio.commands.workerfiles.CommandGenerateDebugFiles;
import es.bsc.compss.nio.commands.workerfiles.CommandGenerateDebugFilesDone;
import es.bsc.compss.nio.exceptions.SerializedObjectException;
import es.bsc.compss.nio.requests.DataRequest;
import es.bsc.compss.types.resources.MethodResourceDescription;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Tests the application-level shutdown handshake between master and worker.
 * <h2>What this tests</h2> Exercises the exact COMPSs code path triggered by {@code NIOWorkerNode.stop()} lines
 * 332-338:
 *
 * <pre>
 * Connection c = NIOAgent.getTransferManager().startConnection(node);
 * CommandShutdown cmd = new CommandShutdown(null);
 * NIOAgent.registerOngoingCommand(c, cmd);
 * c.sendCommand(cmd); // causes worker's commandReceived() to fire
 * c.receive(); // registers for ACK — no-op at application level
 * c.finishConnection(); // causes worker's connectionFinished() to fire
 * </pre>
 *
 * The potential race condition: the NIO event loop may dispatch {@code connectionFinished()} before
 * {@code commandReceived()} (TCP FIN can be seen before the application-layer command is dispatched). We verify that
 * the worker's {@code receivedShutdown()} is invoked in all orderings, meaning the application-level handler
 * ({@link NIOHandler}) is correct regardless of arrival order.
 * <h2>What this does NOT test</h2> TCP / Java NIO delivery guarantees. Those are tested at a lower level and are known
 * to be correct. This test exercises only the {@link NIOHandler} → {@link CommandShutdown#handle} →
 * {@link NIOAgent#receivedShutdown} → {@link NIOAgent#shutdown} chain.
 */
public class NioShutdownRaceTest {

    private static final int RACE_ITERATIONS = 500;

    // -------------------------------------------------------------------------
    // Stubs
    // -------------------------------------------------------------------------


    /**
     * Minimal {@link Connection} stub. All mutating calls are no-ops; {@link #hasErrors()} returns {@code false} so
     * that {@link NIOHandler#connectionFinished} proceeds normally.
     */
    private static class StubConnection implements Connection {

        @Override
        public void sendCommand(Object cmd) {
        }

        @Override
        public void sendDataFile(String path) {
        }

        @Override
        public void sendDataFile(String path, boolean compress) {
        }

        @Override
        public void sendDataObject(Object obj) {
        }

        @Override
        public void sendDataArray(byte[] data) {
        }

        @Override
        public void sendDataByteBuffer(ByteBuffer buf) {
        }

        @Override
        public void receive() {
        }

        @Override
        public void receive(String fileName) {
        }

        @Override
        public void receiveDataObject() {
        }

        @Override
        public void receiveDataArray() {
        }

        @Override
        public void receiveDataByteBuffer() {
        }

        @Override
        public void receiveDataFile(String path) {
        }

        @Override
        public void finishConnection() {
        }

        @Override
        public boolean hasErrors() {
            return false;
        }

        @Override
        public Node getNode() {
            return null;
        }
    }

    /**
     * Minimal {@link Transfer} stub that carries a pre-set object (the command). All NIO-level lifecycle methods throw
     * {@link UnsupportedOperationException} — they are never called during these unit tests.
     */
    private static class CommandTransfer extends Transfer {

        CommandTransfer(Object command) {
            super(false);
            this.type = Type.COMMAND;
            this.object = command;
        }

        @Override
        public Direction getDirection() {
            return Direction.RECEIVE;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isSubmission() {
            return false;
        }

        @Override
        public boolean checkViability(boolean b, List<ByteBuffer> in, List<ByteBuffer> out) throws ViabilityException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void start(InternalConnection c, List<ByteBuffer> in, List<ByteBuffer> out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void progress(InternalConnection c, List<ByteBuffer> in, List<ByteBuffer> out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isComplete(List<ByteBuffer> in, List<ByteBuffer> out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void notifyCompletion(InternalConnection c, EventManager em) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void notifyError(InternalConnection c, EventManager em, CommException ex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pause(InternalConnection c) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Minimal {@link NIOAgent} stub. Tracks how many times {@link #shutdown(Connection)} is called. All other abstract
     * methods are no-ops. {@code hasPendingTransfers()} returns {@code false} (the parent's default when constructed
     * with zero send/receive slots and empty queues), so {@link NIOAgent#receivedShutdown} always proceeds directly to
     * {@code shutdown()}.
     */
    private static class ShutdownTrackingAgent extends NIOAgent {

        final AtomicInteger shutdownCount = new AtomicInteger(0);


        ShutdownTrackingAgent() {
            super(0, 0, 0);
        }

        @Override
        public void shutdown(Connection c) {
            shutdownCount.incrementAndGet();
        }

        // --- all remaining abstract methods — stubs, never called in these tests ---

        @Override
        public boolean isPersistentCEnabled() {
            return false;
        }

        @Override
        public void setMaster(NIONode master) {
        }

        @Override
        protected String getPossiblyRenamedFileName(File f, es.bsc.compss.nio.NIOData d) {
            return null;
        }

        @Override
        public boolean isMyUuid(String uuid, String nodeName) {
            return false;
        }

        @Override
        public void setWorkerIsReady(String nodeName) {
        }

        @Override
        public String getWorkingDir() {
            return "";
        }

        @Override
        public void receivedNewTask(NIONode master, NIOTask t, List<String> obsoleteFiles) {
        }

        @Override
        public void receivedNewDataFetchOrder(NIOParam data, int transferId) {
        }

        @Override
        public Object getObject(String s) throws SerializedObjectException {
            return null;
        }

        @Override
        public String getObjectAsFile(String name) {
            return null;
        }

        @Override
        protected void handleDataToSendNotAvailable(Connection c, es.bsc.compss.nio.NIOData d) {
        }

        @Override
        public void handleRequestedDataNotAvailableError(List<DataRequest> failedRequests, String dataId) {
        }

        @Override
        public void receivedBindingObjectAsFile(String filename, String targetPath) {
        }

        @Override
        public void receivedValue(Destination type, String dataId, Object object, List<DataRequest> achievedRequests) {
        }

        @Override
        public void copiedData(int transferGroupId) {
        }

        @Override
        public void receivedNIOTaskDone(Connection c, NIOTaskResult tr, NIOTaskProfile profile, boolean successful,
            Exception e) {
        }

        @Override
        public void shutdownNotification(Connection c) {
        }

        @Override
        public void shutdownExecutionManager(Connection c) {
        }

        @Override
        public void shutdownExecutionManagerNotification(Connection c) {
        }

        @Override
        public void generateDebugFiles(Connection c) {
        }

        @Override
        public void generateAnalysisFiles(Connection c) {
        }

        @Override
        public void notifyDebugFilesDone(Set<String> logPath) {
        }

        @Override
        public void notifyAnalysisFilesDone(Set<String> tracingFilesPaths) {
        }

        @Override
        public void increaseResources(MethodResourceDescription description) {
        }

        @Override
        public void reduceResources(MethodResourceDescription description) {
        }

        @Override
        public void performedResourceUpdate(Connection c) {
        }

        @Override
        public void cancelRunningTask(NIONode node, int jobId) {
        }

        @Override
        public void unhandeledError(Connection c) {
        }

        @Override
        public void handleCancellingTaskCommandError(Connection c, CommandCancelTask cmd) {
        }

        @Override
        public void handleDataReceivedCommandError(Connection c, CommandDataReceived cmd) {
        }

        @Override
        public void handleExecutorShutdownCommandError(Connection c, CommandExecutorShutdown cmd) {
        }

        @Override
        public void handleExecutorShutdownCommandACKError(Connection c, CommandExecutorShutdownACK cmd) {
        }

        @Override
        public void handleTaskDoneCommandError(Connection c, CommandNIOTaskDone cmd) {
        }

        @Override
        public void handleNewTaskCommandError(Connection c, CommandNewTask cmd) {
        }

        @Override
        public void handleShutdownCommandError(Connection c, CommandShutdown cmd) {
        }

        @Override
        public void handleShutdownACKCommandError(Connection c, CommandShutdownACK cmd) {
        }

        @Override
        public void handleTracingGenerateDoneCommandError(Connection c, CommandGenerateAnalysisFilesDone cmd) {
        }

        @Override
        public void handleTracingGenerateCommandError(Connection c, CommandGenerateAnalysisFiles cmd) {
        }

        @Override
        public void handleGenerateWorkerDebugCommandError(Connection c, CommandGenerateDebugFiles cmd) {
        }

        @Override
        public void handleGenerateWorkerDebugDoneCommandError(Connection c, CommandGenerateDebugFilesDone cmd) {
        }

        @Override
        public void receivedRemoveObsoletes(NIONode node, List<String> obsolete) {
        }

        @Override
        public void handleRemoveObsoletesCommandError(Connection c, CommandRemoveObsoletes cmd) {
        }

        @Override
        public void workerPongReceived(String nodeName) {
        }

        @Override
        public void handleNodeIsDownError(String nodeName) {
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------


    /**
     * Builds a fresh agent + handler pair for each test to avoid shared state.
     */
    private static ShutdownTrackingAgent newAgent() {
        return new ShutdownTrackingAgent();
    }

    private static NIOHandler newHandler(NIOAgent agent) {
        return new NIOHandler(agent);
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Normal ordering: commandReceived fires before connectionFinished. This is the expected production path when the
     * command bytes arrive before TCP FIN.
     */
    @Test
    public void normalOrder_commandReceivedThenConnectionFinished() {
        ShutdownTrackingAgent agent = newAgent();
        NIOHandler handler = newHandler(agent);
        StubConnection conn = new StubConnection();
        CommandTransfer transfer = new CommandTransfer(new CommandShutdown(null));

        NIOAgent.registerOngoingCommand(conn, new CommandShutdown(null));

        handler.commandReceived(conn, transfer);
        handler.connectionFinished(conn);

        assertEquals("shutdown() must be called exactly once in normal order", 1, agent.shutdownCount.get());
    }

    /**
     * Reversed ordering: connectionFinished fires before commandReceived. Models the race where TCP FIN is processed
     * before the command bytes are dispatched at the application level. The worker must still receive the
     * CommandShutdown.
     */
    @Test
    public void reversedOrder_connectionFinishedThenCommandReceived() {
        ShutdownTrackingAgent agent = newAgent();
        NIOHandler handler = newHandler(agent);
        StubConnection conn = new StubConnection();
        CommandTransfer transfer = new CommandTransfer(new CommandShutdown(null));

        NIOAgent.registerOngoingCommand(conn, new CommandShutdown(null));

        // connectionFinished arrives first (FIN before command dispatch)
        handler.connectionFinished(conn);
        // commandReceived still fires because the bytes were already in the kernel buffer
        handler.commandReceived(conn, transfer);

        assertEquals("shutdown() must be called exactly once even when connectionFinished arrives first", 1,
            agent.shutdownCount.get());
    }

    /**
     * Concurrent race: commandReceived and connectionFinished are dispatched simultaneously from two threads, modelling
     * the real NIO event loop where both events can race. Verifies over {@value #RACE_ITERATIONS} iterations that
     * shutdown() is always called.
     */
    @Test(timeout = 30_000)
    public void concurrentRace_commandAndConnectionFinishRace() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        int missed = 0;

        try {
            for (int i = 0; i < RACE_ITERATIONS; i++) {
                ShutdownTrackingAgent agent = newAgent();
                NIOHandler handler = newHandler(agent);
                StubConnection conn = new StubConnection();
                CommandTransfer transfer = new CommandTransfer(new CommandShutdown(null));

                NIOAgent.registerOngoingCommand(conn, new CommandShutdown(null));

                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(2);

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    handler.commandReceived(conn, transfer);
                    done.countDown();
                });

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    handler.connectionFinished(conn);
                    done.countDown();
                });

                ready.await();
                start.countDown();
                done.await(1, TimeUnit.SECONDS);

                if (agent.shutdownCount.get() != 1) {
                    missed++;
                }
            }
        } finally {
            pool.shutdownNow();
        }
        assertTrue("pool did not terminate cleanly", pool.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals("shutdown() must be called exactly once in every iteration regardless of event ordering", 0,
            missed);
    }
}
