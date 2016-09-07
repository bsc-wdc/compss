package integratedtoolkit.nio.dataRequest;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.nio.commands.Data;
import integratedtoolkit.types.data.operation.DataOperation;


public class MasterDataRequest extends DataRequest {

    private final DataOperation fOp;


    public MasterDataRequest(DataOperation fOp, DataType type, Data source, String target) {
        super(type, source, target);
        this.fOp = fOp;
    }

    public DataOperation getOperation() {
        return this.fOp;
    }

}
