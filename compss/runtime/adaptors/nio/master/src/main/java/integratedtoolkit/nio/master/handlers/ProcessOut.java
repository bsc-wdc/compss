package integratedtoolkit.nio.master.handlers;

public class ProcessOut {

    private StringBuffer output = new StringBuffer();
    private StringBuffer error = new StringBuffer();
    private int exitValue = -1;


    /*
     * GETTERS
     */
    public int getExitValue() {
        return this.exitValue;
    }

    public String getOutput() {
        return this.output.toString();
    }

    public String getError() {
        return this.error.toString();
    }

    /*
     * SETTERS
     */
    public void setExitValue(int exit) {
        exitValue = exit;
    }

    public void appendError(String line) {
        error.append(line);
    }

    public void appendOutput(String line) {
        output.append(line + "\n");
    }

}