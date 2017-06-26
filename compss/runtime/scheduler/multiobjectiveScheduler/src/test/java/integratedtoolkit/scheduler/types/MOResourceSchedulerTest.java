package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.multiobjective.MOResourceScheduler;
import integratedtoolkit.scheduler.multiobjective.types.MOProfile;
import integratedtoolkit.scheduler.types.fake.FakeWorker;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.components.Processor;
import integratedtoolkit.util.CoreManager;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class MOResourceSchedulerTest {

    private static final double DEFAULT_IDLE_POWER = 1;
    private static final double DEFAULT_IDLE_PRICE = 0;
    private static final long DEFAULT_MIN_EXECUTION_TIME = Long.MAX_VALUE;
    private static final long DEFAULT_AVG_EXECUTION_TIME = 100l;
    private static final long DEFAULT_MAX_EXECUTION_TIME = Long.MIN_VALUE;
    private static final long DEFAULT_EXECUTION_COUNT = 0l;
    private static final double DEFAULT_POWER = 0l;
    private static final double DEFAULT_PRICE = 0l;

    private static final double SET_IDLE_POWER = 100;
    private static final double SET_IDLE_PRICE = 0.1;
    private static final long SET_MIN_EXECUTION_TIME = 2;
    private static final long SET_AVG_EXECUTION_TIME = 5;
    private static final long SET_MAX_EXECUTION_TIME = 10;
    private static final long SET_EXECUTION_COUNT = 3l;
    private static final double SET_POWER = 4.3;
    private static final double SET_PRICE = 0.45;

    private static final String SET_PROFILE = "{\"maxTime\":" + (SET_MAX_EXECUTION_TIME) + ",\"executions\":" + (SET_EXECUTION_COUNT)
            + ",\"avgTime\":" + (SET_AVG_EXECUTION_TIME) + ",\"minTime\":" + (SET_MIN_EXECUTION_TIME) + ",\"power\":" + (SET_POWER)
            + ",\"price\":" + (SET_PRICE) + "}";
    private static FakeWorker worker;

    // Test Logger
    private static final Logger LOGGER = LogManager.getLogger("Console");


    @BeforeClass
    public static void setUpClass() {
        LOGGER.debug("Setup Class");
        // Method resource description and its slots
        Processor p = new Processor();
        p.setComputingUnits(4);
        MethodResourceDescription description = new MethodResourceDescription();
        description.addProcessor(p);
        worker = new FakeWorker(description, 4);

        int coreId = CoreManager.registerNewCoreElement("methodA");
        LinkedList<Implementation> impls = new LinkedList<>();
        LinkedList<String> signs = new LinkedList<>();
        Implementation impl = new MethodImplementation("ClassA", "methodA", coreId, 0, new MethodResourceDescription());
        impls.add(impl);
        signs.add("ClassA.methodA");
        impl = new MethodImplementation("ClassB", "methodA", coreId, 1, new MethodResourceDescription());
        impls.add(impl);
        signs.add("ClassB.methodA");
        CoreManager.registerNewImplementations(coreId, impls, signs);

        coreId = CoreManager.registerNewCoreElement("methodB");
        impls = new LinkedList<>();
        signs = new LinkedList<>();
        impl = new MethodImplementation("ClassA", "methodB", coreId, 0, new MethodResourceDescription());
        impls.add(impl);
        signs.add("ClassA.methodB");
        CoreManager.registerNewImplementations(coreId, impls, signs);
    }

    @AfterClass
    public static void tearDownClass() {
        LOGGER.debug("End");
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testNull() {
        MOResourceScheduler<MethodResourceDescription> rs = new MOResourceScheduler<MethodResourceDescription>(worker, null);
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on null test");
                }
            }
        }
        if (rs.getIdlePower() != DEFAULT_IDLE_POWER) {
            fail("Invalid idle Power for Null constructor");
        }
        if (rs.getIdlePrice() != DEFAULT_IDLE_PRICE) {
            fail("Invalid idle Price for Null constructor");
        }
    }

    @Test
    public void testEmpty() {
        MOResourceScheduler<MethodResourceDescription> rs = new MOResourceScheduler<MethodResourceDescription>(worker,
                new JSONObject("{\"implementations\":{}}"));
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on empty test");
                }
            }
        }
        if (rs.getIdlePower() != DEFAULT_IDLE_POWER) {
            fail("Invalid idle Power for Null constructor");
        }
        if (rs.getIdlePrice() != DEFAULT_IDLE_PRICE) {
            fail("Invalid idle Price for Null constructor");
        }
    }

    @Test
    public void testAllSetNoPrice() {
        MOResourceScheduler<MethodResourceDescription> rs = new MOResourceScheduler<MethodResourceDescription>(worker,
                new JSONObject("{\"idlePrice\": " + SET_IDLE_PRICE + ", \"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + ","
                        + "\"ClassB.methodA\":" + SET_PROFILE + "," + "\"ClassA.methodB\":" + SET_PROFILE + "}}"));
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on all set no-price test");
                }
            }
        }
        if (rs.getIdlePower() != DEFAULT_IDLE_POWER) {
            fail("Invalid idle Power for all set constructor");
        }
        if (rs.getIdlePrice() != SET_IDLE_PRICE) {
            fail("Invalid idle Price for all set constructor");
        }
    }

    @Test
    public void testAllSetNoPower() {
        MOResourceScheduler<MethodResourceDescription> rs = new MOResourceScheduler<MethodResourceDescription>(worker,
                new JSONObject("{\"idlePrice\": " + SET_IDLE_PRICE + ", \"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + ","
                        + "\"ClassB.methodA\":" + SET_PROFILE + "," + "\"ClassA.methodB\":" + SET_PROFILE + "}}"));
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on all set no-power  test");
                }
            }
        }
        if (rs.getIdlePower() != DEFAULT_IDLE_POWER) {
            fail("Invalid idle Power for all set constructor");
        }
        if (rs.getIdlePrice() != SET_IDLE_PRICE) {
            fail("Invalid idle Price for all set constructor");
        }
    }

    @Test
    public void testAllSet() {
        MOResourceScheduler<MethodResourceDescription> rs = new MOResourceScheduler<MethodResourceDescription>(worker,
                new JSONObject("{\"idlePower\": " + SET_IDLE_POWER + ", \"idlePrice\": " + SET_IDLE_PRICE
                        + ", \"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + "," + "\"ClassB.methodA\":" + SET_PROFILE + ","
                        + "\"ClassA.methodB\":" + SET_PROFILE + "}}"));
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on all set test");
                }
            }
        }
        if (rs.getIdlePower() != SET_IDLE_POWER) {
            fail("Invalid idle Power for all set constructor");
        }
        if (rs.getIdlePrice() != SET_IDLE_PRICE) {
            fail("Invalid idle Price for all set constructor");
        }
    }

    @Test
    public void testAllSetCopy() {
        MOResourceScheduler<MethodResourceDescription> rs = new MOResourceScheduler<MethodResourceDescription>(worker,
                new JSONObject("{\"idlePower\": " + SET_IDLE_POWER + ", \"idlePrice\": " + SET_IDLE_PRICE
                        + ", \"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + "," + "\"ClassB.methodA\":" + SET_PROFILE + ","
                        + "\"ClassA.methodB\":" + SET_PROFILE + "}}"));
        JSONObject jo = rs.toJSONObject();
        rs = new MOResourceScheduler<MethodResourceDescription>(worker, jo);
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on all set test");
                }
            }
        }
        if (rs.getIdlePower() != SET_IDLE_POWER) {
            fail("Invalid idle Power for copy test");
        }
        if (rs.getIdlePrice() != SET_IDLE_PRICE) {
            fail("Invalid idle Price for copy test");
        }
    }

    private void checkSetProfile(MOProfile p) throws CheckerException {
        if (p.getExecutionCount() != SET_EXECUTION_COUNT) {
            throw new CheckerException("execution count");
        }
        if (p.getMinExecutionTime() != SET_MIN_EXECUTION_TIME) {
            throw new CheckerException("min execution time");
        }
        if (p.getAverageExecutionTime() != SET_AVG_EXECUTION_TIME) {
            throw new CheckerException("max average time");
        }
        if (p.getMaxExecutionTime() != SET_MAX_EXECUTION_TIME) {
            throw new CheckerException("max execution time");
        }
        if (p.getPower() != SET_POWER) {
            throw new CheckerException("power");
        }
        if (p.getPrice() != SET_PRICE) {
            throw new CheckerException("price");
        }
    }

    private void checkUnsetProfile(MOProfile p) throws CheckerException {
        if (p.getExecutionCount() != DEFAULT_EXECUTION_COUNT) {
            throw new CheckerException("execution count");
        }
        if (p.getMinExecutionTime() != DEFAULT_MIN_EXECUTION_TIME) {
            throw new CheckerException("min execution time");
        }
        if (p.getAverageExecutionTime() != DEFAULT_AVG_EXECUTION_TIME) {
            throw new CheckerException("max average time");
        }
        if (p.getMaxExecutionTime() != DEFAULT_MAX_EXECUTION_TIME) {
            throw new CheckerException("max execution time");
        }
        if (p.getPower() != DEFAULT_POWER) {
            throw new CheckerException("power");
        }
        if (p.getPrice() != DEFAULT_PRICE) {
            throw new CheckerException("price");
        }
    }


    private class CheckerException extends Exception {

        /**
         * Runtime exceptions are always 2L
         */
        private static final long serialVersionUID = 2L;

        private final String feature;


        public CheckerException(String feature) {
            this.feature = feature;
        }

        public String getFeature() {
            return feature;
        }

    }
}
