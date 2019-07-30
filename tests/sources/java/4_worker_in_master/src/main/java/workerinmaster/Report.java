
package workerinmaster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class Report implements Externalizable {

    private long startTime;
    private long endTime;


    public Report() {
        this.startTime = System.currentTimeMillis();
    }

    public void completedExecution() {
        this.endTime = System.currentTimeMillis();
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeLong(startTime);
        oo.writeLong(endTime);
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        this.startTime = oi.readLong();
        this.endTime = oi.readLong();
    }

}
