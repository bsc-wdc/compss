package integratedtoolkit.scheduler.types;

import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;


public class ProfileTest {

    private static final long DEFAULT_MIN_EXECUTION_TIME = Long.MAX_VALUE;
    private static final long DEFAULT_AVG_EXECUTION_TIME = 100l;
    private static final long DEFAULT_MAX_EXECUTION_TIME = Long.MIN_VALUE;
    private static final long DEFAULT_EXECUTION_COUNT = 0l;

    private static final long SET_MIN_EXECUTION_TIME = 2;
    private static final long SET_AVG_EXECUTION_TIME = 5;
    private static final long SET_MAX_EXECUTION_TIME = 10;
    private static final long SET_EXECUTION_COUNT = 3l;


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
        Profile p = new Profile();
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
    }

    @Test
    public void testEmpty() {
        Profile p = new Profile();
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
    }

    @Test
    public void testEmptyJSON() {
        Profile p = new Profile(new JSONObject("{}"));
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
    }

    @Test
    public void testCompleteJSON() {
        Profile p = new Profile(new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":" + SET_MAX_EXECUTION_TIME
                + "," + "\"minTime\":" + SET_MIN_EXECUTION_TIME + "," + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "}"));
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
    }

    @Test
    public void testNoExecCountJSON() {
        Profile p = new Profile(new JSONObject("{" + "\"maxTime\":" + SET_MAX_EXECUTION_TIME + "," + "\"minTime\":" + SET_MIN_EXECUTION_TIME
                + "," + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "}"));
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
    }

    @Test
    public void testNoMinJSON() {
        Profile p = new Profile(new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":" + SET_MAX_EXECUTION_TIME
                + "," + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "}"));
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
    }

    @Test
    public void testNoAverageJSON() {
        Profile p = new Profile(new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":" + SET_MAX_EXECUTION_TIME
                + "," + "\"minTime\":" + SET_MIN_EXECUTION_TIME + "}"));
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
    }

    @Test
    public void testNoMaxJSON() {
        Profile p = new Profile(new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"minTime\":" + SET_MIN_EXECUTION_TIME
                + "," + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "}"));
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
    }

    @Test
    public void toJSONObjectTest() {
        Profile original = new Profile(
                new JSONObject("{" + "\"executions\":" + SET_EXECUTION_COUNT + "," + "\"maxTime\":" + SET_MAX_EXECUTION_TIME + ","
                        + "\"minTime\":" + SET_MIN_EXECUTION_TIME + "," + "\"avgTime\":" + SET_AVG_EXECUTION_TIME + "}"));
        JSONObject jsonObject = original.toJSONObject();
        Profile p = new Profile(jsonObject);
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
    }
}
