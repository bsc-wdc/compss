package benchmarks;

public class AdaptorTestResultEntry {

    private boolean result;

    private long time;

    private Throwable e;

    public AdaptorTestResultEntry(boolean result, long time, Throwable e) {
        this.result = result;
        this.time = time;
        this.e = e;
    }

    public boolean getResult() {
        return result;
    }

    public long getTime() {
        return time;
    }

    public Throwable getException() {
        return e;
    }

}
