package integratedtoolkit.nio.dataRequest;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.commands.Data;


public class WorkerDataRequest extends DataRequest {

    private final TransferringTask task;


    public WorkerDataRequest(TransferringTask task, DataType type, Data source, String target) {
        super(type, source, target);
        this.task = task;
    }

    public TransferringTask getTransferringTask() {
        return this.task;
    }


    public static class TransferringTask {

        private final NIOTask task;
        private int params;
        private boolean error;


        public TransferringTask(NIOTask task) {
            this.task = task;
            params = task.getParams().size();
        }

        public NIOTask getTask() {
            return this.task;
        }

        public int getParams() {
            return this.params;
        }

        public boolean getError() {
            return this.error;
        }

        public void decreaseParams() {
            --this.params;
        }

        public void setError(boolean error) {
            this.error = error;
        }
    }

}