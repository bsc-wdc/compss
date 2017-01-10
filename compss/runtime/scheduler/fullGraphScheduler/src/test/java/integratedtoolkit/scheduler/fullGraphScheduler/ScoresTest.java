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
import integratedtoolkit.scheduler.types.fake.FakeAllocatableAction;
import integratedtoolkit.scheduler.types.fake.FakeImplementation;
import integratedtoolkit.scheduler.types.fake.FakeProfile;
import integratedtoolkit.scheduler.types.fake.FakeResourceDescription;
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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class ScoresTest {

    private static FullGraphScheduler ds;
    private static FullGraphResourceScheduler drs;
    private static FullGraphResourceScheduler secondDRS;

    private static long CORE0;
    private static long CORE2;
    private static long CORE4_0;
    private static long CORE4_1;


    public ScoresTest() {

    }

    @BeforeClass
    public static void setUpClass() {
        CoreManager.clear();
        CoreManager.resizeStructures(5);

        System.setProperty(ITConstants.IT_TRACING, "0");
        System.setProperty(ITConstants.IT_EXTRAE_CONFIG_FILE, "");
        Comm.init();

        ds = new FullGraphScheduler();

        Implementation<?> impl00 = new FakeImplementation(0, 0, new FakeResourceDescription(2));
        CoreManager.registerImplementations(0, new Implementation[] { impl00 }, new String[] { "fakeSignature00" });
        Implementation<?> impl10 = new FakeImplementation(1, 0, new FakeResourceDescription(3));
        CoreManager.registerImplementations(1, new Implementation[] { impl10 }, new String[] { "fakeSignature10" });
        Implementation<?> impl20 = new FakeImplementation(2, 0, new FakeResourceDescription(1));
        CoreManager.registerImplementations(2, new Implementation[] { impl20 }, new String[] { "fakeSignature20" });
        Implementation<?> impl30 = new FakeImplementation(3, 0, new FakeResourceDescription(4));
        CoreManager.registerImplementations(3, new Implementation[] { impl30 }, new String[] { "fakeSignature30" });
        Implementation<?> impl40 = new FakeImplementation(4, 0, new FakeResourceDescription(4));
        Implementation<?> impl41 = new FakeImplementation(4, 1, new FakeResourceDescription(2));
        CoreManager.registerImplementations(4, new Implementation[] { impl40, impl41 }, new String[] { "fakeSignature40", "fakeSignature41" });

        int maxSlots = 4;
        FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
        FakeWorker fw = new FakeWorker("worker1", frd, maxSlots);
        drs = new FullGraphResourceScheduler(fw);

        FakeResourceDescription frd2 = new FakeResourceDescription(maxSlots);
        FakeWorker fw2 = new FakeWorker("worker2", frd2, maxSlots);
        secondDRS = new FullGraphResourceScheduler(fw2);

        drs.profiledExecution(impl00, new FakeProfile(50));
        drs.profiledExecution(impl10, new FakeProfile(50));
        drs.profiledExecution(impl20, new FakeProfile(30));
        drs.profiledExecution(impl30, new FakeProfile(50));
        drs.profiledExecution(impl40, new FakeProfile(20));
        drs.profiledExecution(impl41, new FakeProfile(30));

        CORE0 = drs.getProfile(impl00).getAverageExecutionTime();
        CORE2 = drs.getProfile(impl20).getAverageExecutionTime();
        CORE4_0 = drs.getProfile(impl40).getAverageExecutionTime();
        CORE4_1 = drs.getProfile(impl41).getAverageExecutionTime();

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

    @Test
    public void testActionScores() throws BlockedActionException, UnassignedActionException {
        drs.clear();
        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 1, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action2 = new FakeAllocatableAction(2, 0, CoreManager.getCoreImplementations(0));

        FakeAllocatableAction action14 = new FakeAllocatableAction(14, 0, CoreManager.getCoreImplementations(0));
        action14.selectExecution(secondDRS, action14.getImplementations()[0]);
        FullGraphSchedulingInformation dsi14 = (FullGraphSchedulingInformation) action14.getSchedulingInfo();
        dsi14.setExpectedEnd(10_000);

        FakeAllocatableAction action15 = new FakeAllocatableAction(15, 0, CoreManager.getCoreImplementations(0));
        action15.selectExecution(secondDRS, action15.getImplementations()[0]);
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
        drs.clear();
        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(0));

        DataInstanceId d1v1 = new DataInstanceId(1, 1);
        Comm.registerData(d1v1.getRenaming());
        DataInstanceId d2v2 = new DataInstanceId(2, 2);
        Comm.registerData(d2v2.getRenaming());

        DependencyParameter dpD1V1 = new DependencyParameter(DataType.FILE_T, Direction.IN, Stream.UNSPECIFIED, Constants.PREFIX_EMTPY);
        dpD1V1.setDataAccessId(new RAccessId(1, 1));

        DependencyParameter dpD2V2 = new DependencyParameter(DataType.FILE_T, Direction.IN, Stream.UNSPECIFIED, Constants.PREFIX_EMTPY);
        dpD2V2.setDataAccessId(new RAccessId(2, 2));

        TaskDescription params = new TaskDescription("", "", false, Constants.SINGLE_NODE, false, false, false, 
                new Parameter[] { dpD1V1, dpD2V2 });
        FullGraphScore actionScore = (FullGraphScore) ds.generateActionScore(action1);

        FullGraphScore score1 = (FullGraphScore) drs.generateResourceScore(action1, params, actionScore);
        Verifiers.verifyScore(score1, 0, 2 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY, 0, 0,
                2 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY);

        Comm.registerLocation(d1v1.getRenaming(), DataLocation.createLocation(drs.getResource(), new SimpleURI("/home/test/a")));
        score1 = (FullGraphScore) drs.generateResourceScore(action1, params, actionScore);
        Verifiers.verifyScore(score1, 0, 1 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY, 0, 0,
                1 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY);

        Comm.registerLocation(d2v2.getRenaming(), DataLocation.createLocation(drs.getResource(), new SimpleURI("/home/test/b")));
        score1 = (FullGraphScore) drs.generateResourceScore(action1, params, actionScore);
        Verifiers.verifyScore(score1, 0, 0 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY, 0, 0,
                0 * FullGraphResourceScheduler.DATA_TRANSFER_DELAY);

        Comm.removeData(d1v1.getRenaming());
        Comm.removeData(d2v2.getRenaming());
    }

    @Test
    public void testImplementationScores() throws BlockedActionException, UnassignedActionException {

        drs.clear();
        // No resources and no dependencies
        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(4));
        TaskDescription tp1 = new TaskDescription("", "", false, Constants.SINGLE_NODE, false, false, false, new Parameter[0]);
        FullGraphScore score1 = (FullGraphScore) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 0, 0, 0, 0, 0);

        FullGraphScore score1_0 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 0, 0, CORE4_0, 0);
        FullGraphScore score1_1 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 0, 0, CORE4_1, 0);
        Verifiers.validateBetterScore(score1_0, score1_1, true);

        // Resources with load
        FakeAllocatableAction action2 = new FakeAllocatableAction(2, 0, CoreManager.getCoreImplementations(0));
        action2.selectExecution(drs, action2.getImplementations()[0]);
        drs.initialSchedule(action2);
        score1_0 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 0, CORE0, CORE4_0, CORE0);
        score1_1 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 0, 0, CORE4_1, 0);
        Verifiers.validateBetterScore(score1_0, score1_1, false);

        FakeAllocatableAction action3 = new FakeAllocatableAction(3, 0, CoreManager.getCoreImplementations(2));
        action3.selectExecution(drs, action3.getImplementations()[0]);
        drs.initialSchedule(action3);
        score1_0 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 0, CORE0, CORE4_0, CORE0);
        score1_1 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 0, CORE2, CORE4_1, CORE2);
        Verifiers.validateBetterScore(score1_0, score1_1, false);

        // Data Dependencies
        FakeAllocatableAction action10 = new FakeAllocatableAction(10, 0, CoreManager.getCoreImplementations(0));
        action10.selectExecution(secondDRS, action10.getImplementations()[0]);
        FullGraphSchedulingInformation dsi10 = (FullGraphSchedulingInformation) action10.getSchedulingInfo();
        dsi10.setExpectedEnd(10);
        action1.addDataPredecessor(action10);
        score1 = (FullGraphScore) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 0, 10, 0, 0, 10);

        score1_0 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 10, CORE0, CORE4_0, CORE0);
        score1_1 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 10, CORE2, CORE4_1, CORE2);
        Verifiers.validateBetterScore(score1_0, score1_1, false);

        FakeAllocatableAction action11 = new FakeAllocatableAction(11, 0, CoreManager.getCoreImplementations(0));
        action11.selectExecution(secondDRS, action11.getImplementations()[0]);
        FullGraphSchedulingInformation dsi11 = (FullGraphSchedulingInformation) action11.getSchedulingInfo();
        dsi11.setExpectedEnd(10_000);
        action1.addDataPredecessor(action11);
        score1 = (FullGraphScore) ds.generateActionScore(action1);
        Verifiers.verifyScore(score1, 0, 10_000, 0, 0, 10_000);
        score1_0 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[0], score1);
        Verifiers.verifyScore(score1_0, 0, 10_000, CORE0, CORE4_0, 10_000);
        score1_1 = (FullGraphScore) drs.generateImplementationScore(action1, tp1, action1.getImplementations()[1], score1);
        Verifiers.verifyScore(score1_1, 0, 10_000, CORE2, CORE4_1, 10_000);
        Verifiers.validateBetterScore(score1_0, score1_1, true);

    }

}
