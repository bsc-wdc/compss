package es.bsc.compss.invokers.util;

public class StdIOStream {

    private String stdIn = null;
    private String stdOut = null;
    private String stdErr = null;


    public StdIOStream() {
        // Nothing to do since all attributes have been initialized
    }

    public String getStdIn() {
        return stdIn;
    }

    public String getStdOut() {
        return stdOut;
    }

    public String getStdErr() {
        return stdErr;
    }

    public void setStdIn(String stdIn) {
        this.stdIn = stdIn;
    }

    public void setStdOut(String stdOut) {
        this.stdOut = stdOut;
    }

    public void setStdErr(String stdErr) {
        this.stdErr = stdErr;
    }

}
