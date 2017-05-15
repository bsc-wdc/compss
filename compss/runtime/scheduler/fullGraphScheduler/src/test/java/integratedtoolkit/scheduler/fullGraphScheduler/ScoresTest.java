package integratedtoolkit.scheduler.fullGraphScheduler;

import integratedtoolkit.ITConstants;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphScheduler;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import integratedtoolkit.scheduler.fullGraphScheduler.utils.Verifiers;
import integratedtoolkit.scheduler.types.FullGraphScore;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.scheduler.types.fake.FakeActionOrchestrator;
import integratedtoolkit.scheduler.types.fake.FakeAllocatableAction;
import integratedtoolkit.scheduler.types.fake.FakeImplementation;
import integratedtoolkit.scheduler.types.fake.FakeProfile;
import integratedtoolkit.scheduler.types.fake.FakeResourceDescription;
import integratedtoolkit.scheduler.types.fake.FakeResourceScheduler;
import integratedtoolkit.scheduler.types.fake.FakeWorker;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;
import integratedtoolkit.types.data.DataAccessId.RAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.CoreManager;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class ScoresTest {

    private static FullGraphScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> ds;
    private static FakeActionOrchestrator fao;
    private static FakeResourceScheduler drs1;
    private static FakeResourceScheduler drs2;

    private static long CORE0;
    private static long CORE2;
    private static long CORE4_0;
    private static long CORE4_1;


    public ScoresTest() {
        ds = new FullGraphScheduler<FakeProfile, FakeResourceDescription, FakeImplementation>();
        fao = new FakeActionOrchestrator(ds);
        ds.setOrchestrator(fao);
    }

    @BeforeClass
    public static void setUpClass() {
        CoreManager.clear();
        CoreManager.registerNewCoreElement("fakeSignature00");
        CoreManager.registerNewCoreElement("fakeSignature10");
        CoreManager.registerNewCoreElement("fakeSignature20");
        CoreManager.registerNewCoreElement("fakeSignature30");
        CoreManager.registerNewCoreElement("fakeSignature40");
        CoreManager.registerNewCoreElement("task");

        System.setProperty(ITConstants.IT_TRACING, "0");
        System.setProperty(ITConstants.IT_EXTRAE_CONFIG_FILE, "");
        Comm.init();

        ds = new FullGraphScheduler<FakeProfile, FakeResourceDescription, FakeImplementation>();

        FakeImplementation impl00 = new FakeImplementation(0, 0, new FakeResourceDescription(2));
        List<Implementation<?>> impls0 = new LinkedList<>();
        impls0.add(impl00);
        List<String> signatures0 = new LinkedList<>();
        signatures0.add("fakeSignature00");
        CoreManager.registerNewImplementations(0, impls0, signatures0);

        FakeImplementation impl10 = new FakeImplementation(1, 0, new FakeResourceDescription(3));
        List<Implementation<?>> impls1 = new LinkedList<>();
        impls1.add(impl10);
        List<String> signatures1 = new LinkedList<>();
        signatures1.add("fakeSignature10");
        CoreManager.registerNewImplementations(1, impls1, signatures1);

        FakeImplementation impl20 = new FakeImplementation(2, 0, new FakeResourceDescription(1));
        List<Implementation<?>> impls2 = new LinkedList<>();
        impls2.add(impl20);
        List<String> signatures2 = new LinkedList<>();
        signatures2.add("fakeSignature20");
        CoreManager.registerNewImplementations(2, impls2, signatures2);

        FakeImplementation impl30 = new FakeImplementation(3, 0, new FakeResourceDescription(4));
        List<Implementation<?>> impls3 = new LinkedList<>();
        impls3.add(impl30);
        List<String> signatures3 = new LinkedList<>();
        signatures3.add("fakeSignature30");
        CoreManager.registerNewImplementations(3, impls3, signatures3);

        FakeImplementation impl40 = new FakeImplementation(4, 0, new FakeResourceDescription(4));
        FakeImplementation impl41 = new FakeImplementation(4, 1, new FakeResourceDescription(2));
        List<Implementation<?>> impls4 = new LinkedList<>();
        impls4.add(impl40);
        impls4.add(impl41);
        List<String> signatures4 = new LinkedList<>();
        signatures4.add("fakeSignature40");
        signatures4.add("fakeSignature41");
        CoreManager.registerNewImplementations(4, impls4, signatures4);

        int maxSlots = 4;
        FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
        FakeWorker fw = new FakeWorker("worker1", frd, maxSlots);
        drs1 = new FakeResourceScheduler(fw, fao, 0);

        FakeResourceDescription frd2 = new FakeResourceDescription(maxSlots);
        FakeWorker fw2 = new FakeWorker("worker2", frd2, maxSlots);
        drs2 = new FakeResourceScheduler(fw2, fao, 0);

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
    }

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testActionScores() throws BlockedActionException, UnassignedActionException {
        drs1.clear();
        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 1, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, CoreManager.getCoreImplementations(0));

        FakeAllocatableAction action14 = new FakeAllocatableAction(fao, 14, 0, CoreManager.getCoreImplementations(0));
        action14.selectExecution(drs2, (FakeImplementation) action14.getImplementations()[0]);
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> dsi14 = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action14
                .getSchedulingInfo();
        dsi14.setExpectedEnd(10_000);

        FakeAllocatableAction action15 = new FakeAllocatableAction(fao, 15, 0, CoreManager.getCoreImplementations(0));
        action15.selectExecution(drs2, (FakeImplementation) action15.getImplementations()[0]);
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> dsi15 = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action15
                .getSchedulingInfo();
        dsi15.setExpectedEnd(12_000);

        FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation> score1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) ds
                .generateActionScore(action1);
        Score score2 = ds.generateActionScore(action2);
        Verifiers.verifyScore(score1, 1, 0, 0, 0, 0);
        Verifiers.verifyScore((FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) score2, 0, 0, 0, 0, 0);
        Verifiers.validateBetterScore(score1, score2, true);

        action1.addDataPredecessor(action14);
        score1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 1, 10_000, 0, 0, 10_000);

        action1.addDataPredecessor(action15);
        score1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 1, 12_000, 0, 0, 12_000);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResourceScores() throws BlockedActionException, UnassignedActionException, Exception {
        drs1.clear();
        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(0));

        DataInstanceId d1v1 = new DataInstanceId(1, 1);
        Comm.registerData(d1v1.getRenaming());
        DataInstanceId d2v2 = new DataInstanceId(2, 2);
        Comm.registerData(d2v2.getRenaming());

        DependencyParameter dpD1V1 = new DependencyParameter(DataType.FILE_T, Direction.IN, Stream.UNSPECIFIED, Constants.PREFIX_EMTPY);
        dpD1V1.setDataAccessId(new RAccessId(1, 1));

        DependencyParameter dpD2V2 = new DependencyParameter(DataType.FILE_T, Direction.IN, Stream.UNSPECIFIED, Constants.PREFIX_EMTPY);
        dpD2V2.setDataAccessId(new RAccessId(2, 2));

        TaskDescription params = new TaskDescription("task", false, Constants.SINGLE_NODE, false, false, false, false,
                new Parameter[] { dpD1V1, dpD2V2 });
        FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation> actionScore = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) ds
                .generateActionScore(action1);

        FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation> score1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1
                .generateResourceScore(action1, params, actionScore);
        Verifiers.verifyScore(score1, 0, 2 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY, 0, 0,
                2 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY);

        Comm.registerLocation(d1v1.getRenaming(), DataLocation.createLocation(drs1.getResource(), new SimpleURI("/home/test/a")));
        score1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateResourceScore(action1, params,
                actionScore);
        Verifiers.verifyScore(score1, 0, 1 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY, 0, 0,
                1 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY);

        Comm.registerLocation(d2v2.getRenaming(), DataLocation.createLocation(drs1.getResource(), new SimpleURI("/home/test/b")));
        score1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateResourceScore(action1, params,
                actionScore);
        Verifiers.verifyScore(score1, 0, 0 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY, 0, 0,
                0 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY);

        Comm.removeData(d1v1.getRenaming());
        Comm.removeData(d2v2.getRenaming());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testImplementationScores() throws BlockedActionException, UnassignedActionException {
        drs1.clear();
        // No resources and no dependencies
        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(4));
        TaskDescription tp1 = new TaskDescription("task", false, Constants.SINGLE_NODE, false, false, false, false, new Parameter[0]);
        FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation> score1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) ds
                .generateActionScore(action1);
        Verifiers.verifyScore(score1, 0, 0, 0, 0, 0);

        FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation> score1_0 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1
                .generateImplementationScore(action1, tp1, action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 0, 0, CORE4_0, 0);
        FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation> score1_1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1
                .generateImplementationScore(action1, tp1, action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 0, 0, CORE4_1, 0);
        Verifiers.validateBetterScore(score1_0, score1_1, true);

        // Resources with load
        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, CoreManager.getCoreImplementations(0));
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        score1_0 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 0, CORE0, CORE4_0, CORE0);
        score1_1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 0, 0, CORE4_1, 0);
        Verifiers.validateBetterScore(score1_0, score1_1, false);

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 0, CoreManager.getCoreImplementations(2));
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);
        score1_0 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 0, CORE0, CORE4_0, CORE0);
        score1_1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 0, CORE2, CORE4_1, CORE2);
        Verifiers.validateBetterScore(score1_0, score1_1, false);

        // Data Dependencies
        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, CoreManager.getCoreImplementations(0));
        action10.selectExecution(drs2, (FakeImplementation) action10.getImplementations()[0]);
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> dsi10 = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action10
                .getSchedulingInfo();
        dsi10.setExpectedEnd(10);
        action1.addDataPredecessor(action10);
        score1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 0, 10, 0, 0, 10);

        score1_0 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 10, CORE0, CORE4_0, CORE0);
        score1_1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 10, CORE2, CORE4_1, CORE2);
        Verifiers.validateBetterScore(score1_0, score1_1, false);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, CoreManager.getCoreImplementations(0));
        action11.selectExecution(drs2, (FakeImplementation) action11.getImplementations()[0]);
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> dsi11 = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action11
                .getSchedulingInfo();
        dsi11.setExpectedEnd(10_000);
        action1.addDataPredecessor(action11);
        score1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 0, 10_000, 0, 0, 10_000);
        score1_0 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 10_000, CORE0, CORE4_0, 10_000);
        score1_1 = (FullGraphScore<FakeProfile, FakeResourceDescription, FakeImplementation>) drs1.generateImplementationScore(action1, tp1,
                action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 10_000, CORE2, CORE4_1, 10_000);
        Verifiers.validateBetterScore(score1_0, score1_1, true);
    }

}
