package es.bsc.compss.nio.dataRequest;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.nio.commands.Data;
import es.bsc.compss.types.data.operation.DataOperation;


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
