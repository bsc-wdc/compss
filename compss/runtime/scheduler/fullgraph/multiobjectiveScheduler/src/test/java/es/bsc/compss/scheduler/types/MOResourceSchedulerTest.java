/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.types;

import static org.junit.Assert.fail;

import es.bsc.compss.scheduler.fullgraph.multiobjective.MOResourceScheduler;
import es.bsc.compss.scheduler.fullgraph.multiobjective.types.MOProfile;
import es.bsc.compss.scheduler.types.fake.FakeWorker;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.definition.ContainerDescription;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.util.CoreManager;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class MOResourceSchedulerTest {

    private static final String METHOD = MethodType.METHOD.toString();
    private static final double DEFAULT_IDLE_POWER = 1;
    private static final double DEFAULT_IDLE_PRICE = 0;
    private static final long DEFAULT_MIN_EXECUTION_TIME = Long.MAX_VALUE;
    private static final long DEFAULT_AVG_EXECUTION_TIME = 100L;
    private static final long DEFAULT_MAX_EXECUTION_TIME = Long.MIN_VALUE;
    private static final long DEFAULT_EXECUTION_COUNT = 0L;
    private static final double DEFAULT_POWER = 0L;
    private static final double DEFAULT_PRICE = 0L;

    private static final double SET_IDLE_POWER = 100;
    private static final double SET_IDLE_PRICE = 0.1;
    private static final long SET_MIN_EXECUTION_TIME = 2;
    private static final long SET_AVG_EXECUTION_TIME = 5;
    private static final long SET_MAX_EXECUTION_TIME = 10;
    private static final long SET_EXECUTION_COUNT = 3L;
    private static final double SET_POWER = 4.3;
    private static final double SET_PRICE = 0.45;

    private static final String SET_PROFILE = "{\"maxTime\":" + (SET_MAX_EXECUTION_TIME) + ",\"executions\":"
        + (SET_EXECUTION_COUNT) + ",\"avgTime\":" + (SET_AVG_EXECUTION_TIME) + ",\"minTime\":"
        + (SET_MIN_EXECUTION_TIME) + ",\"power\":" + (SET_POWER) + ",\"price\":" + (SET_PRICE) + "}";
    private static FakeWorker worker;

    // Test Logger
    private static final Logger LOGGER = LogManager.getLogger("Console");


    /**
     * Sets up the test class.
     */
    @BeforeClass
    public static void setUpClass() {
        LOGGER.info("Setup Class");
        // Method resource description and its slots
        Processor p = new Processor();
        p.setComputingUnits(4);
        MethodResourceDescription description = new MethodResourceDescription();
        description.addProcessor(p);
        worker = new FakeWorker(description, 4);

        CoreElementDefinition cedA = new CoreElementDefinition();
        cedA.setCeSignature("methodA");
        ImplementationDescription<?, ?> implDef = null;

        implDef = ImplementationDescription.defineImplementation(METHOD, "ClassA.methodA", false,
            new MethodResourceDescription(), null, null, null, "ClassA", "methodA");
        cedA.addImplementation(implDef);
        implDef = ImplementationDescription.defineImplementation(METHOD, "ClassB.methodA", false,
            new MethodResourceDescription(), null, null, null, "ClassB", "methodA");
        cedA.addImplementation(implDef);
        CoreManager.registerNewCoreElement(cedA);

        CoreElementDefinition cedB = new CoreElementDefinition();
        cedB.setCeSignature("methodB");
        implDef = ImplementationDescription.defineImplementation(METHOD, "ClassA.methodB", false,
            new MethodResourceDescription(), null, null, null, "ClassA", "methodB");
        cedB.addImplementation(implDef);
        CoreManager.registerNewCoreElement(cedB);
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
        MOResourceScheduler<MethodResourceDescription> rs =
            new MOResourceScheduler<MethodResourceDescription>(worker, null, null);
        for (CoreElement ce : CoreManager.getAllCores()) {
            List<Implementation> impls = ce.getImplementations();
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException cEx) {
                    fail("Invalid " + cEx.getFeature() + " for unset implementation " + impl.getImplementationId()
                        + " core " + impl.getCoreId() + " on null test");
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
            new JSONObject("{\"implementations\":{}}"), null);
        for (CoreElement ce : CoreManager.getAllCores()) {
            List<Implementation> impls = ce.getImplementations();
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkUnsetProfile(p);
                } catch (CheckerException cEx) {
                    fail("Invalid " + cEx.getFeature() + " for unset implementation " + impl.getImplementationId()
                        + " core " + impl.getCoreId() + " on empty test");
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
            new JSONObject(
                "{\"idlePrice\": " + SET_IDLE_PRICE + ", \"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + ","
                    + "\"ClassB.methodA\":" + SET_PROFILE + "," + "\"ClassA.methodB\":" + SET_PROFILE + "}}"),
            null);
        for (CoreElement ce : CoreManager.getAllCores()) {
            List<Implementation> impls = ce.getImplementations();
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException cEx) {
                    fail("Invalid " + cEx.getFeature() + " for unset implementation " + impl.getImplementationId()
                        + " core " + impl.getCoreId() + " on all set no-price test");
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
            new JSONObject(
                "{\"idlePrice\": " + SET_IDLE_PRICE + ", \"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + ","
                    + "\"ClassB.methodA\":" + SET_PROFILE + "," + "\"ClassA.methodB\":" + SET_PROFILE + "}}"),
            null);
        for (CoreElement ce : CoreManager.getAllCores()) {
            List<Implementation> impls = ce.getImplementations();
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException cEx) {
                    fail("Invalid " + cEx.getFeature() + " for unset implementation " + impl.getImplementationId()
                        + " core " + impl.getCoreId() + " on all set no-power  test");
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
                + ", \"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + "," + "\"ClassB.methodA\":" + SET_PROFILE
                + "," + "\"ClassA.methodB\":" + SET_PROFILE + "}}"),
            null);
        for (CoreElement ce : CoreManager.getAllCores()) {
            List<Implementation> impls = ce.getImplementations();
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException cEx) {
                    fail("Invalid " + cEx.getFeature() + " for unset implementation " + impl.getImplementationId()
                        + " core " + impl.getCoreId() + " on all set test");
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
                + ", \"implementations\":{\"ClassA.methodA\":" + SET_PROFILE + "," + "\"ClassB.methodA\":" + SET_PROFILE
                + "," + "\"ClassA.methodB\":" + SET_PROFILE + "}}"),
            null);
        JSONObject jo = rs.toJSONObject();
        rs = new MOResourceScheduler<MethodResourceDescription>(worker, jo, null);
        for (CoreElement ce : CoreManager.getAllCores()) {
            List<Implementation> impls = ce.getImplementations();
            for (Implementation impl : impls) {
                MOProfile p = (MOProfile) rs.getProfile(impl);
                try {
                    checkSetProfile(p);
                } catch (CheckerException cEx) {
                    fail("Invalid " + cEx.getFeature() + " for unset implementation " + impl.getImplementationId()
                        + " core " + impl.getCoreId() + " on all set test");
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
         * Runtime exceptions are always 2L.
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
