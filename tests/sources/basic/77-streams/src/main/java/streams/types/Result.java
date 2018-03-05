package streams.types;

import java.io.Serializable;


public class Result implements Serializable {

    /**
     * Serial ID
     */
    private static final long serialVersionUID = 4L;

    private static int NUM_VALUES;
    private static double ACCUM_VALUE;

    private final int numValues;
    private final double accumValue;
    private final Integer exitValue;


    /**
     * Generates a new result from the static information
     * 
     * @param exitValue
     * @return
     */
    public static Result generate(int exitValue) {
        Result r = new Result(exitValue);
        return r;
    }

    /**
     * Adds a new value to the static information
     * 
     * @param value
     */
    public static void addValue(double value) {
        ACCUM_VALUE += value;
        ++NUM_VALUES;
    }

    /**
     * Creates a new Result instance for externalization
     * 
     */
    public Result() {
        // Only for externalization
        this.numValues = 0;
        this.accumValue = 0;
        this.exitValue = -1;
    }

    /**
     * Creates a new result class and initializes to 0
     * 
     */
    public Result(int exitValue) {
        this.numValues = NUM_VALUES;
        this.accumValue = ACCUM_VALUE;
        this.exitValue = exitValue;
    }

    /**
     * Returns the number of recorded messages
     * 
     * @return
     */
    public int getNumMessages() {
        return this.numValues;
    }

    /**
     * Returns the accumulated value of all recorded messages
     * 
     * @return
     */
    public double getAccumValue() {
        return this.accumValue;
    }

    /**
     * Returns the exit value associated to the result. 0 for all ok, -1 for non finished, other values for errors
     * 
     * @return
     */
    public Integer getExitValue() {
        return this.exitValue;
    }

}
