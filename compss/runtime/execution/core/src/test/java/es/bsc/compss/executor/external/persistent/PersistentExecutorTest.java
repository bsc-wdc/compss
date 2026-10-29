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
package es.bsc.compss.executor.external.persistent;

import es.bsc.compss.executor.external.ExternalExecutor;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the PersistentExecutor handle: construction state, contract conformance and the transitions its mirror
 * drives. The full lifecycle state machine is owned by ExternalExecutor and covered by PipedExecutorTest.
 */
public class PersistentExecutorTest {

    @Test
    public void isAnExternalExecutor() {
        PersistentExecutor executor = new PersistentExecutor(null, null);

        Assert.assertTrue(executor instanceof ExternalExecutor);
    }

    @Test
    public void creatingExecutorAcceptsTasks() {
        PersistentExecutor executor = new PersistentExecutor(null, null);

        Assert.assertTrue(executor.canRun());
    }

    @Test
    public void deployedExecutorAcceptsTasks() {
        PersistentExecutor executor = new PersistentExecutor(null, null);

        executor.deployed();

        Assert.assertTrue(executor.canRun());
    }

    @Test
    public void stoppingExecutorRejectsTasks() {
        PersistentExecutor executor = new PersistentExecutor(null, null);
        executor.deployed();

        executor.stopping();

        Assert.assertFalse(executor.canRun());
    }

    @Test
    public void terminatedExecutorRejectsTasks() {
        PersistentExecutor executor = new PersistentExecutor(null, null);
        executor.deployed();

        executor.terminated();

        Assert.assertFalse(executor.canRun());
    }

}
