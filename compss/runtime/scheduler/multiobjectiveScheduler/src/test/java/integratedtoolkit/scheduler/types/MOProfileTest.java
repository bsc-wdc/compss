package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.multiobjective.types.MOProfile;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;


public class MOProfileTest {

    private static final long DEFAULT_MIN_EXECUTION_TIME = Long.MAX_VALUE;
    private static final long DEFAULT_AVG_EXECUTION_TIME = 100l;
    private static final long DEFAULT_MAX_EXECUTION_TIME = Long.MIN_VALUE;
    private static final long DEFAULT_EXECUTION_COUNT = 0l;
    private static final double DEFAULT_POWER = 0l;
    private static final double DEFAULT_PRICE = 0l;

    private static final long SET_MIN_EXECUTION_TIME = 2;
    private static final long SET_AVG_EXECUTION_TIME = 5;
    private static final long SET_MAX_EXECUTION_TIME = 10;
    private static final long SET_EXECUTION_COUNT = 3l;
    private static final double SET_POWER = 4.3;
    private static final double SET_PRICE = 0.45;


    @BeforeClass
    public static void setUpClass() {
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
        MOProfile p = new MOProfile(null);
        if (p.getExecutionCount() != DEFAULT_EXECUTION_COUNT) {
            fail("Invalid default execution count on null constructor");
        }
        if (p.getMinExecutionTime() != DEFAULT_MIN_EXECUTION_TIME) {
            fail("Invalid default min execution time on null constructor");
        }
        if (p.getAverageExecutionTime() != DEFAULT_AVG_EXECUTION_TIME) {
            fail("Invalid default average execution time on null constructor");
        }
        if (p.getMaxExecutionTime() != DEFAULT_MAX_EXECUTION_TIME) {
            fail("Invalid default max execution time on null constructor");
        }
        if (p.getPower() != DEFAULT_POWER) {
            fail("Invalid default power on null constructor");
        }
        if (p.getPrice() != DEFAULT_PRICE) {
            fail("Invalid default price on null constructor");
        }
    }

    @Test
    public void testEmpty() {
        MOProfile p = new MOProfile();
        if (p.getExecutionCount() != DEFAULT_EXECUTION_COUNT) {
            fail("Invalid default execution count on empty constructor");
        }
        if (p.getMinExecutionTime() != DEFAULT_MIN_EXECUTION_TIME) {
            fail("Invalid default min execution time on empty constructor");
        }
        if (p.getAverageExecutionTime() != DEFAULT_AVG_EXECUTION_TIME) {
            fail("Invalid default average execution time on empty constructor");
        }
        if (p.getMaxExecutionTime() != DEFAULT_MAX_EXECUTION_TIME) {
            fail("Invalid default max execution time on empty constructor");
        }
        if (p.getPower() != DEFAULT_POWER) {
            fail("Invalid default power on empty constructor");
        }
        if (p.getPrice() != DEFAULT_PRICE) {
            fail("Invalid default price on empty constructor");
        }
    }

    @Test
    public void testEmptyJSON() {
        MOProfile p = new MOProfile(new JSONObject("{}"));
        if (p.getExecutionCount() != DEFAULT_EXECUTION_COUNT) {
            fail("Invalid default execution count on empty JSON constructor");
        }
        if (p.getMinExecutionTime() != DEFAULT_MIN_EXECUTION_TIME) {
            fail("Invalid default min execution time on empty JSON constructor");
        }
        if (p.getAverageExecutionTime() != DEFAULT_AVG_EXECUTION_TIME) {
            fail("Invalid default average execution time on empty JSON constructor");
        }
        if (p.getMaxExecutionTime() != DEFAULT_MAX_EXECUTION_TIME) {
            fail("Invalid default max execution time on empty JSON constructor");
        }
        if (p.getPower() != DEFAULT_POWER) {
            fail("Invalid default power on empty JSON constructor");
        }
        if (p.getPrice() != DEFAULT_PRICE) {
            fail("Invalid default price on empty JSON constructor");
        }
    }

    @Test
    public void testCompleteJSON() {
        MOProfile p = new MOProfile(new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":"
                + SET_MAX_EXECUTION_TIME + "," + "\"minTime\":" + SET_MIN_EXECUTION_TIME + "," + "\"avgTime\":" + SET_AVG_EXECUTION_TIME
                + "," + "\"power\":" + SET_POWER + "," + "\"price\":" + SET_PRICE + "}"));
        if (p.getExecutionCount() != SET_EXECUTION_COUNT) {
            fail("Invalid  execution count on complete JSON constructor");
        }
        if (p.getMinExecutionTime() != SET_MIN_EXECUTION_TIME) {
            fail("Invalid default min execution time on complete JSON constructor");
        }
        if (p.getAverageExecutionTime() != SET_AVG_EXECUTION_TIME) {
            fail("Invalid default average execution time on complete JSON constructor");
        }
        if (p.getMaxExecutionTime() != SET_MAX_EXECUTION_TIME) {
            fail("Invalid default max execution time on complete JSON constructor");
        }
        if (p.getPower() != SET_POWER) {
            fail("Invalid default power on complete JSON constructor");
        }
        if (p.getPrice() != SET_PRICE) {
            fail("Invalid default price on complete JSON constructor");
        }
    }

    @Test
    public void testNoExecCountJSON() {
        MOProfile p = new MOProfile(
                new JSONObject("{" + "\"maxTime\":" + SET_MAX_EXECUTION_TIME + "," + "\"minTime\":" + SET_MIN_EXECUTION_TIME + ","
                        + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "," + "\"power\":" + SET_POWER + "," + "\"price\":" + SET_PRICE + "}"));
        if (p.getExecutionCount() != DEFAULT_EXECUTION_COUNT) {
            fail("Invalid  execution count on no execution count JSON constructor");
        }
        if (p.getMinExecutionTime() != SET_MIN_EXECUTION_TIME) {
            fail("Invalid min execution time on no execution count JSON constructor");
        }
        if (p.getAverageExecutionTime() != SET_AVG_EXECUTION_TIME) {
            fail("Invalid average execution time on no execution count JSON constructor");
        }
        if (p.getMaxExecutionTime() != SET_MAX_EXECUTION_TIME) {
            fail("Invalid max execution time on no execution count JSON constructor");
        }
        if (p.getPower() != SET_POWER) {
            fail("Invalid power on no execution count JSON constructor");
        }
        if (p.getPrice() != SET_PRICE) {
            fail("Invalid price on no execution count JSON constructor");
        }
    }

    @Test
    public void testNoMinJSON() {
        MOProfile p = new MOProfile(
                new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":" + SET_MAX_EXECUTION_TIME + ","
                        + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "," + "\"power\":" + SET_POWER + "," + "\"price\":" + SET_PRICE + "}"));
        if (p.getExecutionCount() != SET_EXECUTION_COUNT) {
            fail("Invalid execution count on no min JSON constructor");
        }
        if (p.getMinExecutionTime() != DEFAULT_MIN_EXECUTION_TIME) {
            fail("Invalid min execution time on no min JSON constructor");
        }
        if (p.getAverageExecutionTime() != SET_AVG_EXECUTION_TIME) {
            fail("Invalid average execution time on no min JSON constructor");
        }
        if (p.getMaxExecutionTime() != SET_MAX_EXECUTION_TIME) {
            fail("Invalid max execution time on no min JSON constructor");
        }
        if (p.getPower() != SET_POWER) {
            fail("Invalid power on no min JSON constructor");
        }
        if (p.getPrice() != SET_PRICE) {
            fail("Invalid price on no min JSON constructor");
        }
    }

    @Test
    public void testNoAverageJSON() {
        MOProfile p = new MOProfile(
                new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":" + SET_MAX_EXECUTION_TIME + ","
                        + "\"minTime\":" + SET_MIN_EXECUTION_TIME + "," + "\"power\":" + SET_POWER + "," + "\"price\":" + SET_PRICE + "}"));
        if (p.getExecutionCount() != SET_EXECUTION_COUNT) {
            fail("Invalid execution count on no average JSON constructor");
        }
        if (p.getMinExecutionTime() != SET_MIN_EXECUTION_TIME) {
            fail("Invalid min execution time on no average JSON constructor");
        }
        if (p.getAverageExecutionTime() != DEFAULT_AVG_EXECUTION_TIME) {
            fail("Invalid average execution time on no average JSON constructor");
        }
        if (p.getMaxExecutionTime() != SET_MAX_EXECUTION_TIME) {
            fail("Invalid max execution time on no average JSON constructor");
        }
        if (p.getPower() != SET_POWER) {
            fail("Invalid power on no average JSON constructor");
        }
        if (p.getPrice() != SET_PRICE) {
            fail("Invalid price on no average JSON constructor");
        }
    }

    @Test
    public void testNoMaxJSON() {
        MOProfile p = new MOProfile(
                new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"minTime\":" + SET_MIN_EXECUTION_TIME + ","
                        + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "," + "\"power\":" + SET_POWER + "," + "\"price\":" + SET_PRICE + "}"));
        if (p.getExecutionCount() != SET_EXECUTION_COUNT) {
            fail("Invalid execution count on no max JSON constructor");
        }
        if (p.getMinExecutionTime() != SET_MIN_EXECUTION_TIME) {
            fail("Invalid min execution time on no max JSON constructor");
        }
        if (p.getAverageExecutionTime() != SET_AVG_EXECUTION_TIME) {
            fail("Invalid average execution time on no max JSON constructor");
        }
        if (p.getMaxExecutionTime() != DEFAULT_MAX_EXECUTION_TIME) {
            fail("Invalid max execution time on no max JSON constructor");
        }
        if (p.getPower() != SET_POWER) {
            fail("Invalid power on no max JSON constructor");
        }
        if (p.getPrice() != SET_PRICE) {
            fail("Invalid price on no max JSON constructor");
        }
    }

    @Test
    public void testNoPowerJSON() {
        MOProfile p = new MOProfile(new JSONObject(
                "{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":" + SET_MAX_EXECUTION_TIME + "," + "\"minTime\":"
                        + SET_MIN_EXECUTION_TIME + "," + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "," + "\"price\":" + SET_PRICE + "}"));
        if (p.getExecutionCount() != SET_EXECUTION_COUNT) {
            fail("Invalid execution count on no max JSON constructor");
        }
        if (p.getMinExecutionTime() != SET_MIN_EXECUTION_TIME) {
            fail("Invalid min execution time on no max JSON constructor");
        }
        if (p.getAverageExecutionTime() != SET_AVG_EXECUTION_TIME) {
            fail("Invalid average execution time on no power JSON constructor");
        }
        if (p.getMaxExecutionTime() != SET_MAX_EXECUTION_TIME) {
            fail("Invalid max execution time on no power JSON constructor");
        }
        if (p.getPower() != DEFAULT_POWER) {
            fail("Invalid power on no power JSON constructor");
        }
        if (p.getPrice() != SET_PRICE) {
            fail("Invalid price on no power JSON constructor");
        }
    }

    @Test

    public void testNoPriceJSON() {
        MOProfile p = new MOProfile(new JSONObject(
                "{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":" + SET_MAX_EXECUTION_TIME + "," + "\"minTime\":"
                        + SET_MIN_EXECUTION_TIME + "," + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "," + "\"power\":" + SET_POWER + "}"));
        if (p.getExecutionCount() != SET_EXECUTION_COUNT) {
            fail("Invalid execution count on no price JSON constructor");
        }
        if (p.getMinExecutionTime() != SET_MIN_EXECUTION_TIME) {
            fail("Invalid min execution time on no price JSON constructor");
        }
        if (p.getAverageExecutionTime() != SET_AVG_EXECUTION_TIME) {
            fail("Invalid average execution time on no price JSON constructor");
        }
        if (p.getMaxExecutionTime() != SET_MAX_EXECUTION_TIME) {
            fail("Invalid max execution time on no price JSON constructor");
        }
        if (p.getPower() != SET_POWER) {
            fail("Invalid power on no price JSON constructor");
        }
        if (p.getPrice() != DEFAULT_PRICE) {
            fail("Invalid price on no price JSON constructor");
        }
    }

    @Test
    public void toJSONObjectTest() {
        MOProfile original = new MOProfile(new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":"
                + SET_MAX_EXECUTION_TIME + "," + "\"minTime\":" + SET_MIN_EXECUTION_TIME + "," + "\"avgTime\":" + SET_AVG_EXECUTION_TIME
                + "," + "\"power\":" + SET_POWER + "," + "\"price\":" + SET_PRICE + "}"));
        JSONObject jsonObject = original.toJSONObject();
        MOProfile p = new MOProfile(jsonObject);
        if (p.getExecutionCount() != SET_EXECUTION_COUNT) {
            fail("Invalid  execution count on complete JSON constructor");
        }
        if (p.getMinExecutionTime() != SET_MIN_EXECUTION_TIME) {
            fail("Invalid default min execution time on complete JSON constructor");
        }
        if (p.getAverageExecutionTime() != SET_AVG_EXECUTION_TIME) {
            fail("Invalid default average execution time on complete JSON constructor");
        }
        if (p.getMaxExecutionTime() != SET_MAX_EXECUTION_TIME) {
            fail("Invalid default max execution time on complete JSON constructor");
        }
        if (p.getPower() != SET_POWER) {
            fail("Invalid default power on complete JSON constructor");
        }
        if (p.getPrice() != SET_PRICE) {
            fail("Invalid default price on complete JSON constructor");
        }
    }
}
