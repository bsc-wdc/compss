
package workerinmaster;

public class WorkerModification {

    private final long time;
    private final int change;


    public WorkerModification(long time, int change) {
        this.time = time;
        this.change = change;
    }

    public int getChange() {
        return change;
    }

    public long getTime() {
        return time;
    }

}
