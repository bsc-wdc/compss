/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.fullgraph;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.fullgraph.FullGraphResourceScheduler;
import es.bsc.compss.scheduler.fullgraph.FullGraphScheduler;
import es.bsc.compss.scheduler.fullgraph.FullGraphSchedulingInformation;
import es.bsc.compss.scheduler.fullgraph.utils.Verifiers;
import es.bsc.compss.scheduler.types.FullGraphScore;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.scheduler.types.fake.FakeActionOrchestrator;
import es.bsc.compss.scheduler.types.fake.FakeAllocatableAction;
import es.bsc.compss.scheduler.types.fake.FakeImplDefinition;
import es.bsc.compss.scheduler.types.fake.FakeImplementation;
import es.bsc.compss.scheduler.types.fake.FakeProfile;
import es.bsc.compss.scheduler.types.fake.FakeResourceDescription;
import es.bsc.compss.scheduler.types.fake.FakeResourceScheduler;
import es.bsc.compss.scheduler.types.fake.FakeWorker;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ResourceManager;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class ScoresTest {

    private static FullGraphScheduler ds;
    private static FakeActionOrchestrator fao;
    private static FakeResourceScheduler drs1;
    private static FakeResourceScheduler drs2;

    private static long CORE0;
    private static long CORE2;
    private static long CORE4_0;
    private static long CORE4_1;


    /**
     * Tests the scores.
     */
    public ScoresTest() {
        ds = new FullGraphScheduler();
        fao = new FakeActionOrchestrator(ds);
        ds.setOrchestrator(fao);
    }

    /**
     * To setup the class.
     */
    @BeforeClass
    public static void setUpClass() {
        ResourceManager.clear(null);

        CoreManager.clear();

        System.setProperty(COMPSsConstants.TRACING, "0");
        System.setProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, "");

        // TODO: Comm should be initialized?
        // Comm.init();
        ds = new FullGraphScheduler();

        CoreElementDefinition ced;
        FakeImplDefinition fid;

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature00");
        fid = new FakeImplDefinition("fakeSignature00", new FakeResourceDescription(2));
        ced.addImplementation(fid);
        CoreElement ce0 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl00 = ce0.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature10");
        fid = new FakeImplDefinition("fakeSignature10", new FakeResourceDescription(3));
        ced.addImplementation(fid);
        CoreElement ce1 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl10 = ce1.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature20");
        fid = new FakeImplDefinition("fakeSignature20", new FakeResourceDescription(1));
        ced.addImplementation(fid);
        CoreElement ce2 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl20 = ce2.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature30");
        fid = new FakeImplDefinition("fakeSignature30", new FakeResourceDescription(4));
        ced.addImplementation(fid);
        CoreElement ce3 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl30 = ce3.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature40");
        fid = new FakeImplDefinition("fakeSignature40", new FakeResourceDescription(4));
        ced.addImplementation(fid);
        fid = new FakeImplDefinition("fakeSignature41", new FakeResourceDescription(2));
        ced.addImplementation(fid);
        CoreElement ce4 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl40 = ce4.getImplementation(0);
        final Implementation impl41 = ce4.getImplementation(1);

        ced = new CoreElementDefinition();
        ced.setCeSignature("task");
        CoreManager.registerNewCoreElement(ced);

        int maxSlots = 4;
        FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
        FakeWorker fw = new FakeWorker("worker1", frd, maxSlots);
        drs1 = new FakeResourceScheduler(fw, null, null, fao, 0);

        FakeResourceDescription frd2 = new FakeResourceDescription(maxSlots);
        FakeWorker fw2 = new FakeWorker("worker2", frd2, maxSlots);
        drs2 = new FakeResourceScheduler(fw2, null, null, fao, 0);

        drs1.profiledExecution(impl00, new FakeProfile(50));
        drs1.profiledExecution(impl10, new FakeProfile(50));
        drs1.profiledExecution(impl20, new FakeProfile(30));
        drs1.profiledExecution(impl30, new FakeProfile(50));
        drs1.profiledExecution(impl40, new FakeProfile(20));
        drs1.profiledExecution(impl41, new FakeProfile(30));

        CORE0 = drs1.getProfile(impl00).getAverageExecutionTime();
        CORE2 = drs1.getProfile(impl20).getAverageExecutionTime();
        CORE4_0 = drs1.getProfile(impl40).getAverageExecutionTime();
        CORE4_1 = drs1.getProfile(impl41).getAverageExecutionTime();

        // debugConfiguration();
    }

    @AfterClass
    public static void tearDownClass() {
        // Nothing to do
    }

    @Before
    public void setUp() {
        // Nothing to do
    }

    @After
    public void tearDown() {
        // Nothing to do
    }

    @Test
    public void testActionScores() throws BlockedActionException, UnassignedActionException {
        drs1.clear();

        CoreElement ce0 = CoreManager.getCore(0);
        List<Implementation> ce0Impls = ce0.getImplementations();

        final FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 1, ce0Impls);
        final FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, ce0Impls);

        FakeAllocatableAction action14 = new FakeAllocatableAction(fao, 14, 0, ce0Impls);
        action14.selectExecution(drs2, (FakeImplementation) action14.getImplementations()[0]);
        FullGraphSchedulingInformation dsi14 = (FullGraphSchedulingInformation) action14.getSchedulingInfo();
        dsi14.setExpectedEnd(10_000);

        FakeAllocatableAction action15 = new FakeAllocatableAction(fao, 15, 0, ce0Impls);
        action15.selectExecution(drs2, (FakeImplementation) action15.getImplementations()[0]);
        FullGraphSchedulingInformation dsi15 = (FullGraphSchedulingInformation) action15.getSchedulingInfo();
        dsi15.setExpectedEnd(12_000);

        FullGraphScore score1 = (FullGraphScore) ds.generateActionScore(action1);
        Score score2 = ds.generateActionScore(action2);
        Verifiers.verifyScore(score1, 1, 0, 0, 0, 0);
        Verifiers.verifyScore((FullGraphScore) score2, 0, 0, 0, 0, 0);
        Verifiers.validateBetterScore(score1, score2, true);

        action1.addDataPredecessor(action14);
        score1 = (FullGraphScore) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 1, 10_000, 0, 0, 10_000);

        action1.addDataPredecessor(action15);
        score1 = (FullGraphScore) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 1, 12_000, 0, 0, 12_000);
    }

    @Test
    public void testResourceScores() throws BlockedActionException, UnassignedActionException, Exception {
        drs1.clear();

        CoreElement ce0 = CoreManager.getCore(0);
        List<Implementation> ce0Impls = ce0.getImplementations();

        final FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, ce0Impls);

        DataInstanceId d1v1 = new DataInstanceId(1, 1);
        Comm.registerData(d1v1.getRenaming());
        DataInstanceId d2v2 = new DataInstanceId(2, 2);
        Comm.registerData(d2v2.getRenaming());

        DependencyParameter dpD1V1 = new DependencyParameter(DataType.FILE_T, Direction.IN, StdIOStream.UNSPECIFIED,
                Constants.PREFIX_EMPTY, "dp1");
        dpD1V1.setDataAccessId(new RAccessId(1, 1));

        DependencyParameter dpD2V2 = new DependencyParameter(DataType.FILE_T, Direction.IN, StdIOStream.UNSPECIFIED,
                Constants.PREFIX_EMPTY, "dp2");
        dpD2V2.setDataAccessId(new RAccessId(2, 2));

        TaskDescription params = new TaskDescription(TaskType.METHOD, Lang.UNKNOWN, "task", new CoreElement(0, ""),
                false, Constants.SINGLE_NODE, false, false, false, 0, 0, Arrays.asList(dpD1V1, dpD2V2));

        FullGraphScore actionScore = (FullGraphScore) ds.generateActionScore(action1);

        FullGraphScore score1 = (FullGraphScore) drs1.generateResourceScore(action1, params, actionScore);
        Verifiers.verifyScore(score1, 0, 2 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY, 0, 0,
                2 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY);

        Comm.registerLocation(d1v1.getRenaming(),
                DataLocation.createLocation(drs1.getResource(), new SimpleURI("/home/test/a")));
        score1 = (FullGraphScore) drs1.generateResourceScore(action1, params, actionScore);
        Verifiers.verifyScore(score1, 0, 1 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY, 0, 0,
                1 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY);

        Comm.registerLocation(d2v2.getRenaming(),
                DataLocation.createLocation(drs1.getResource(), new SimpleURI("/home/test/b")));
        score1 = (FullGraphScore) drs1.generateResourceScore(action1, params, actionScore);
        Verifiers.verifyScore(score1, 0, 0 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY, 0, 0,
                0 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY);

        Comm.removeData(d1v1.getRenaming());
        Comm.removeData(d2v2.getRenaming());
    }

    @Test
    public void testImplementationScores() throws BlockedActionException, UnassignedActionException {
        drs1.clear();

        CoreElement ce4 = CoreManager.getCore(4);
        List<Implementation> ce4Impls = ce4.getImplementations();

        // No resources and no dependencies
        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, ce4Impls);
        TaskDescription tp1 = new TaskDescription(TaskType.METHOD, Lang.UNKNOWN, "task", new CoreElement(0, ""), false,
                Constants.SINGLE_NODE, false, false, false, 0, 0, new LinkedList<>());
        FullGraphScore score1 = (FullGraphScore) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 0, 0, 0, 0, 0);

        FullGraphScore score10 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score10, 0, 0, 0, CORE4_0, 0);
        FullGraphScore score11 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score11, 0, 0, 0, CORE4_1, 0);
        Verifiers.validateBetterScore(score10, score11, true);

        CoreElement ce0 = CoreManager.getCore(0);
        List<Implementation> ce0Impls = ce0.getImplementations();

        // Resources with load
        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, ce0Impls);
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        score10 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1, action1.getImplementations()[0],
                score1);
        Verifiers.verifyScore(score10, 0, 0, CORE0, CORE4_0, CORE0);
        score11 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1, action1.getImplementations()[1],
                score1);
        Verifiers.verifyScore(score11, 0, 0, 0, CORE4_1, 0);
        Verifiers.validateBetterScore(score10, score11, false);

        CoreElement ce2 = CoreManager.getCore(2);
        List<Implementation> ce2Impls = ce2.getImplementations();

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 0, ce2Impls);
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);
        score10 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1, action1.getImplementations()[0],
                score1);
        Verifiers.verifyScore(score10, 0, 0, CORE0, CORE4_0, CORE0);
        score11 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1, action1.getImplementations()[1],
                score1);
        Verifiers.verifyScore(score11, 0, 0, CORE2, CORE4_1, CORE2);
        Verifiers.validateBetterScore(score10, score11, false);

        // Data Dependencies
        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, ce0Impls);
        action10.selectExecution(drs2, (FakeImplementation) action10.getImplementations()[0]);
        FullGraphSchedulingInformation dsi10 = (FullGraphSchedulingInformation) action10.getSchedulingInfo();
        dsi10.setExpectedEnd(10);
        action1.addDataPredecessor(action10);
        score1 = (FullGraphScore) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 0, 10, 0, 0, 10);

        score10 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1, action1.getImplementations()[0],
                score1);
        Verifiers.verifyScore(score10, 0, 10, CORE0, CORE4_0, CORE0);
        score11 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1, action1.getImplementations()[1],
                score1);
        Verifiers.verifyScore(score11, 0, 10, CORE2, CORE4_1, CORE2);
        Verifiers.validateBetterScore(score10, score11, false);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, ce0Impls);
        action11.selectExecution(drs2, (FakeImplementation) action11.getImplementations()[0]);
        FullGraphSchedulingInformation dsi11 = (FullGraphSchedulingInformation) action11.getSchedulingInfo();
        dsi11.setExpectedEnd(10_000);
        action1.addDataPredecessor(action11);
        score1 = (FullGraphScore) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 0, 10_000, 0, 0, 10_000);
        score10 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1, action1.getImplementations()[0],
                score1);
        Verifiers.verifyScore(score10, 0, 10_000, CORE0, CORE4_0, 10_000);
        score11 = (FullGraphScore) drs1.generateImplementationScore(action1, tp1, action1.getImplementations()[1],
                score1);
        Verifiers.verifyScore(score11, 0, 10_000, CORE2, CORE4_1, 10_000);
        Verifiers.validateBetterScore(score10, score11, true);
    }

}
