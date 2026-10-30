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
package es.bsc.compss.execution.executor.binding.piped;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the PipedExecutor handle state machine (CREATING -> RUNNING -> STOPPING -> DELETED).
 */
public class PipedExecutorTest {

    @Test
    public void creatingExecutorAcceptsTasks() {
        PipedExecutor executor = new PipedExecutor(null, null);

        Assert.assertTrue(executor.canRun());
    }

    @Test
    public void deployedExecutorAcceptsTasks() {
        PipedExecutor executor = new PipedExecutor(null, null);

        executor.deployed();

        Assert.assertTrue(executor.canRun());
    }

    @Test
    public void stoppingExecutorRejectsTasks() {
        PipedExecutor executor = new PipedExecutor(null, null);
        executor.deployed();

        executor.stopping();

        Assert.assertFalse(executor.canRun());
    }

    @Test
    public void terminatedExecutorFromCreatingRejectsTasks() {
        PipedExecutor executor = new PipedExecutor(null, null);

        executor.terminated();

        Assert.assertFalse(executor.canRun());
    }

    @Test
    public void terminatedExecutorFromRunningRejectsTasks() {
        PipedExecutor executor = new PipedExecutor(null, null);
        executor.deployed();

        executor.terminated();

        Assert.assertFalse(executor.canRun());
    }

    @Test
    public void terminatedExecutorFromStoppingRejectsTasks() {
        PipedExecutor executor = new PipedExecutor(null, null);
        executor.deployed();
        executor.stopping();

        executor.terminated();

        Assert.assertFalse(executor.canRun());
    }

    @Test
    public void fullLifecycle() {
        PipedExecutor executor = new PipedExecutor(null, null);
        Assert.assertTrue(executor.canRun());

        executor.deployed();
        Assert.assertTrue(executor.canRun());

        executor.stopping();
        Assert.assertFalse(executor.canRun());

        executor.terminated();
        Assert.assertFalse(executor.canRun());
    }

    @Test
    public void handleExposesItsCollaborators() {
        PipePair pipes = new PipePair("pipedExecutorTest", "executor0", null);
        PipedExecutor executor = new PipedExecutor(null, pipes);

        Assert.assertNull(executor.getMirror());
        Assert.assertSame(pipes, executor.getPipePair());
    }

}
