package integratedtoolkit.scheduler.types;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.types.fake.FakeWorker;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.components.Processor;
import integratedtoolkit.util.CoreManager;
import java.util.LinkedList;
import java.util.List;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class ResourceSchedulerTest {

    private static final long DEFAULT_MIN_EXECUTION_TIME = Long.MAX_VALUE;
    private static final long DEFAULT_AVG_EXECUTION_TIME = 100l;
    private static final long DEFAULT_MAX_EXECUTION_TIME = Long.MIN_VALUE;
    private static final long DEFAULT_EXECUTION_COUNT = 0l;

    private static final long SET_MIN_EXECUTION_TIME = 2;
    private static final long SET_AVG_EXECUTION_TIME = 5;
    private static final long SET_MAX_EXECUTION_TIME = 10;
    private static final long SET_EXECUTION_COUNT = 3l;

    private static final String SET_PROFILE = "{\"maxTime\":" + (SET_MAX_EXECUTION_TIME) + ",\"executions\":" + (SET_EXECUTION_COUNT)
            + ",\"avgTime\":" + (SET_AVG_EXECUTION_TIME) + ",\"minTime\":" + (SET_MIN_EXECUTION_TIME) + "}";
    private static final String SET_AND_UPDATED_PROFILE = "{\"maxTime\":" + (SET_MAX_EXECUTION_TIME + 1) + ",\"executions\":"
            + (SET_EXECUTION_COUNT + 1) + ",\"avgTime\":" + (SET_AVG_EXECUTION_TIME + 1) + ",\"minTime\":" + (SET_MIN_EXECUTION_TIME + 1)
            + "}";

    private static FakeWorker worker;


    @BeforeClass
    public static void setUpClass() {
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
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testNull() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker, null);
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                Profile p = rs.getProfile(impl);
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on null test");
                }
            }
        }
    }

    @Test
    public void testEmpty() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker, new JSONObject("{\"implementations\":{}}"));
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                Profile p = rs.getProfile(impl);
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on empty test");
                }
            }
        }
    }

    @Test
    public void testMethodB() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker,
                new JSONObject("{\"implementations\":{\"ClassA.methodB\":" + SET_PROFILE + "}}"));

        List<Implementation> impls = CoreManager.getCoreImplementations(0);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkUnsetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodB test");
            }
        }
        impls = CoreManager.getCoreImplementations(1);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkSetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for set implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodB test");
            }
        }
    }

    @Test
    public void testMethodBUpdated() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker,
                new JSONObject("{\"implementations\":{\"ClassA.methodB\":" + SET_AND_UPDATED_PROFILE + "}}"));

        List<Implementation> impls = CoreManager.getCoreImplementations(0);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkUnsetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodB updated test");
            }
        }
        impls = CoreManager.getCoreImplementations(1);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkSetAndIncreasedProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for set implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodB updated test");
            }
        }
    }

    @Test
    public void testMethodANullSet() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker,
                new JSONObject("{\"implementations\":{\"ClassB.methodA\":" + SET_PROFILE + "}}"));

        List<Implementation> impls = CoreManager.getCoreImplementations(0);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            if (impl.getImplementationId() == 0) {
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA null-set test");
                }
            } else {
                try {
                    checkSetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for set implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA null-set test");
                }
            }
        }

        impls = CoreManager.getCoreImplementations(1);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkUnsetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodA null-set test");
            }
        }
    }

    @Test
    public void testMethodASetNull() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker,
                new JSONObject("{\"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + "}}"));

        List<Implementation> impls = CoreManager.getCoreImplementations(0);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            if (impl.getImplementationId() == 1) {
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA set-null test");
                }
            } else {
                try {
                    checkSetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for set implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA set-null test");
                }
            }
        }

        impls = CoreManager.getCoreImplementations(1);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkUnsetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodA set-null test");
            }
        }
    }

    @Test
    public void testMethodASetSet() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker, new JSONObject(
                "{\"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + "," + "\"ClassB.methodA\":" + SET_PROFILE + "}}"));

        List<Implementation> impls = CoreManager.getCoreImplementations(0);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkSetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodA set-set test");
            }
        }

        impls = CoreManager.getCoreImplementations(1);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkUnsetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodA set-set test");
            }
        }
    }

    @Test
    public void testMethodAUpdatedNull() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker,
                new JSONObject("{\"implementations\":{\"ClassA.methodA\":" + SET_AND_UPDATED_PROFILE + "}}"));

        List<Implementation> impls = CoreManager.getCoreImplementations(0);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            if (impl.getImplementationId() == 1) {
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA updated-null test");
                }
            } else {
                try {
                    this.checkSetAndIncreasedProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for set implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA updated-null test");
                }
            }
        }

        impls = CoreManager.getCoreImplementations(1);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkUnsetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodA updated-null test");
            }
        }
    }

    @Test
    public void testMethodANullUpdated() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker,
                new JSONObject("{\"implementations\":{\"ClassB.methodA\":" + SET_AND_UPDATED_PROFILE + "}}"));

        List<Implementation> impls = CoreManager.getCoreImplementations(0);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            if (impl.getImplementationId() == 0) {
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA null-updated test");
                }
            } else {
                try {
                    this.checkSetAndIncreasedProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for set implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA null-updated test");
                }
            }
        }

        impls = CoreManager.getCoreImplementations(1);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkUnsetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodA null-updated test");
            }
        }
    }

    @Test
    public void testMethodASetUpdated() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker, new JSONObject(
                "{\"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + "," + "\"ClassB.methodA\":" + SET_AND_UPDATED_PROFILE + "}}"));

        List<Implementation> impls = CoreManager.getCoreImplementations(0);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            if (impl.getImplementationId() == 0) {
                try {
                    checkSetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA set-updated test");
                }
            } else {
                try {
                    this.checkSetAndIncreasedProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for set implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on MethodA set-updated test");
                }
            }
        }

        impls = CoreManager.getCoreImplementations(1);
        for (Implementation impl : impls) {
            Profile p = rs.getProfile(impl);
            try {
                checkUnsetProfile(p);
            } catch (CheckerException ce) {
                fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core " + impl.getCoreId()
                        + " on MethodA set-updated test");
            }
        }
    }

    @Test
    public void testAllSet() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker,
                new JSONObject("{\"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + "," + "\"ClassB.methodA\":" + SET_PROFILE + ","
                        + "\"ClassA.methodB\":" + SET_PROFILE + "}}"));
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                Profile p = rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on all set test");
                }
            }
        }
    }

    @Test
    public void testAllSetCopy() {
        ResourceScheduler<MethodResourceDescription> rs = new ResourceScheduler<>(worker,
                new JSONObject("{\"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + "," + "\"ClassB.methodA\":" + SET_PROFILE + ","
                        + "\"ClassA.methodB\":" + SET_PROFILE + "}}"));
        JSONObject jo = rs.toJSONObject();
        rs = new ResourceScheduler<>(worker, jo);
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                Profile p = rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException ce) {
                    fail("Invalid " + ce.getFeature() + " for unset implementation " + impl.getImplementationId() + " core "
                            + impl.getCoreId() + " on copy test");
                }
            }
        }
    }

    private void checkSetProfile(Profile p) throws CheckerException {
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
    }

    private void checkSetAndIncreasedProfile(Profile p) throws CheckerException {
        if (p.getExecutionCount() != SET_EXECUTION_COUNT + 1) {
            throw new CheckerException("execution count");
        }
        if (p.getMinExecutionTime() != SET_MIN_EXECUTION_TIME + 1) {
            throw new CheckerException("min execution time");
        }
        if (p.getAverageExecutionTime() != SET_AVG_EXECUTION_TIME + 1) {
            throw new CheckerException("max average time");
        }
        if (p.getMaxExecutionTime() != SET_MAX_EXECUTION_TIME + 1) {
            throw new CheckerException("max execution time");
        }
    }

    private void checkUnsetProfile(Profile p) throws CheckerException {
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
    }


    private class CheckerException extends Exception {

        /**
         * All Runtime Exceptions have serial ID 2L
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
