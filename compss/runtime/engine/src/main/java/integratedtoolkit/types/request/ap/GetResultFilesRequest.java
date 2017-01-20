package integratedtoolkit.types.request.ap;

import integratedtoolkit.components.impl.AccessProcessor;
import integratedtoolkit.components.impl.DataInfoProvider;
import integratedtoolkit.components.impl.TaskAnalyser;
import integratedtoolkit.components.impl.TaskDispatcher;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;

import integratedtoolkit.types.data.ResultFile;
import integratedtoolkit.types.data.operation.ResultListener;
import java.util.TreeSet;


public class GetResultFilesRequest extends APRequest {

    private Long appId;
    private Semaphore sem;

    private LinkedList<ResultFile> blockedData;


    public GetResultFilesRequest(Long appId, Semaphore sem) {
        this.appId = appId;
        this.sem = sem;
        blockedData = new LinkedList<>();
    }

    public Long getAppId() {
        return appId;
    }

    public void setAppId(Long appId) {
        this.appId = appId;
    }

    public Semaphore getSemaphore() {
        return sem;
    }

    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    public LinkedList<ResultFile> getBlockedData() {
        return blockedData;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher<?, ?, ?> td) {
        ResultListener listener = new ResultListener(sem);
        TreeSet<Integer> writtenDataIds = ta.getAndRemoveWrittenFiles(this.appId);
        if (writtenDataIds != null) {
            for (int dataId : writtenDataIds) {
                ResultFile rf;
                rf = dip.blockDataAndGetResultFile(dataId, listener);
                if (rf == null) {
                    continue;
                }
                blockedData.add(rf);
            }
            listener.enable();
        } else {
            sem.release();
        }

    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.BLOCK_AND_GET_RESULT_FILES;
    }

}
